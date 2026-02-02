from datetime import datetime
from typing import TYPE_CHECKING, List, Set
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.exc import SQLAlchemyError

from app import logger, scheduler, xray
from app.db import (GetDB, get_notification_reminder,
                    get_users_for_review, update_user_status, get_user_by_id,
                    reset_user_by_next, get_users_for_notification)
from app.db.models import User
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


def process_single_user_review(user_id: int, now_ts: float) -> None:
    
    with GetDB() as db:
        try:
            user = get_user_by_id(db, user_id)
            
            if not user:
                return
            
            limited = user.data_limit and user.used_traffic >= user.data_limit
            expired = user.expire and user.expire <= now_ts
            
            logger.debug(f"Reviewing user \"{user.username}\": limited={limited}, expired={expired}, now={now_ts}, expire={user.expire}, used_traffic={user.used_traffic}, data_limit={user.data_limit}")

            if (limited or expired) and user.next_plan is not None:
                    if user.next_plan.fire_on_either:
                        reset_user_by_next_report(db, user)
                        return

                    elif limited and expired:
                        reset_user_by_next_report(db, user)
                        return

            if limited:
                status = UserStatus.limited
            elif expired:
                status = UserStatus.expired
            else:
                return
            
            xray.operations.remove_user(user)

            update_user_status(db, user, status)

            report.status_change(username=user.username, status=status,
                                 user=UserResponse.model_validate(user), user_admin=user.admin)

            logger.info(f"User \"{user.username}\" status changed to {status}")
        except ObjectDeletedError:
            logger.warning(f"User {user_id} deleted during review, skipping.")
        except SQLAlchemyError as e:
            logger.exception(f"Database error review user {user_id}: {e}")
        except Exception as e:
            logger.exception(f"Unknown error review user {user_id}: {e}")
        finally:
            if user_id in PROCESSING_USERS:
                PROCESSING_USERS.remove(user_id)

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
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            for uid in user_ids_to_review:
                executor.submit(process_single_user_review, uid, now_ts)
    
    if WEBHOOK_ADDRESS:
        with GetDB() as db:
            candidates = get_notification_candidates(db, now_ts)
            for user in candidates:
                add_notification_reminders(db, user, now)


scheduler.add_job(review, 'interval',
                  seconds=JOB_REVIEW_USERS_INTERVAL,
                  coalesce=True, max_instances=1)
