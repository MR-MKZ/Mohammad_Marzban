from datetime import datetime
from typing import TYPE_CHECKING, List, Set
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.orm import Session, joinedload
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.exc import SQLAlchemyError

from app import logger, scheduler, xray
from app.db import (GetDB, get_notification_reminder,
                    get_users_for_review, update_user_status, get_user_by_id,
                    reset_user_by_next, get_users_for_notification)
from app.db.models import User, Proxy
from app.models.user import ReminderType, UserResponse, UserStatus
from app.utils import report
from app.utils.helpers import (calculate_expiration_days,
                               calculate_usage_percent)
from config import (JOB_REVIEW_USERS_INTERVAL, NOTIFY_DAYS_LEFT,
                    NOTIFY_REACHED_USAGE_PERCENT, WEBHOOK_ADDRESS)

if TYPE_CHECKING:
    from app.db.models import User

PROCESSING_USERS: Set[int] = set()

def add_notification_reminders(db: Session, user: "User", now: datetime = datetime.utcnow()) -> None:
    if user.data_limit:
        usage_percent = calculate_usage_percent(user.used_traffic, user.data_limit)

        for percent in sorted(NOTIFY_REACHED_USAGE_PERCENT, reverse=True):
            if usage_percent >= percent:
                if not get_notification_reminder(db, user.id, ReminderType.data_usage, threshold=percent):
                    report.data_usage_percent_reached(
                        db, usage_percent, UserResponse.model_validate(user),
                        user.id, user.expire, threshold=percent
                    )
                break

    if user.expire:
        expire_days = calculate_expiration_days(user.expire)

        for days_left in sorted(NOTIFY_DAYS_LEFT):
            if expire_days <= days_left:
                if not get_notification_reminder(db, user.id, ReminderType.expiration_date, threshold=days_left):
                    report.expire_days_reached(
                        db, expire_days, UserResponse.model_validate(user),
                        user.id, user.expire, threshold=days_left
                    )
                break


def reset_user_by_next_report(db: Session, user: "User"):
    user = reset_user_by_next(db, user)

    xray.operations.update_user(user)

    report.user_data_reset_by_next(user=UserResponse.model_validate(user), user_admin=user.admin)


def process_user_batch(user_ids: List[int], now_ts: float) -> None:
    with GetDB() as db:
        try:
            users = db.query(User).filter(User.id.in_(user_ids)).options(
                joinedload(User.admin),
                joinedload(User.next_plan),
                joinedload(User.proxies).joinedload(Proxy.excluded_inbounds)
            ).all()

            for user in users:
                try:
                    if not user:
                        continue

                    limited = user.data_limit and user.used_traffic >= user.data_limit
                    expired = user.expire and user.expire <= now_ts

                    logger.debug(f"Reviewing user \"{user.username}\": limited={limited}, expired={expired}, now={now_ts}, expire={user.expire}, used_traffic={user.used_traffic}, data_limit={user.data_limit}")

                    if (limited or expired) and user.next_plan is not None:
                            if user.next_plan.fire_on_either:
                                reset_user_by_next_report(db, user)
                                continue

                            elif limited and expired:
                                reset_user_by_next_report(db, user)
                                continue

                    if limited:
                        status = UserStatus.limited
                    elif expired:
                        status = UserStatus.expired
                    else:
                        continue

                    xray.operations.remove_user(user)

                    user.status = status
                    user.last_status_change = datetime.utcnow()

                    report.status_change(username=user.username, status=status,
                                         user=UserResponse.model_validate(user), user_admin=user.admin)

                    logger.info(f"User \"{user.username}\" status changed to {status}")
                except Exception as e:
                    logger.exception(f"Error processing user {user.id} in batch: {e}")

            db.commit()
        except Exception as e:
            logger.exception(f"Error processing batch: {e}")
            db.rollback()
        finally:
            for uid in user_ids:
                PROCESSING_USERS.discard(uid)


def get_notification_candidates(db: Session, now_ts: float) -> List["User"]:
    # check users expiring in the next 30 days
    max_days_lookahead = 30 * 86400
    
    return get_users_for_notification(db, now_ts, max_days_lookahead)

def review():
    now = datetime.utcnow()
    now_ts = now.timestamp()
    
    user_ids_to_review = []
    
    with GetDB() as db:
        # Get only active users who need to be reviewed (limited or expired)
        users = get_users_for_review(db, now_ts)
        for u in users:
            if u.id not in PROCESSING_USERS:
                user_ids_to_review.append(u.id)
                PROCESSING_USERS.add(u.id)
    
    if user_ids_to_review:
        logger.debug(f"Reviewing {len(user_ids_to_review)} users for status change...")
        
        # Batch size of 50
        batch_size = 50
        batches = [user_ids_to_review[i:i + batch_size] for i in range(0, len(user_ids_to_review), batch_size)]

        with ThreadPoolExecutor(max_workers=32) as executor:
            for batch in batches:
                executor.submit(process_user_batch, batch, now_ts)
    
    if WEBHOOK_ADDRESS:
        with GetDB() as db:
            candidates = get_notification_candidates(db, now_ts)
            for user in candidates:
                add_notification_reminders(db, user, now)


scheduler.add_job(review, 'interval',
                  seconds=JOB_REVIEW_USERS_INTERVAL,
                  coalesce=True, max_instances=1)
