from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.exc import SQLAlchemyError

from app import logger, scheduler, xray
from app.db import (GetDB, get_notification_reminder, get_users,
                    get_users_for_review, update_user_status,
                    reset_user_by_next)
from app.models.user import ReminderType, UserResponse, UserStatus
from app.utils import report
from app.utils.helpers import (calculate_expiration_days,
                               calculate_usage_percent)
from config import (JOB_REVIEW_USERS_INTERVAL, NOTIFY_DAYS_LEFT,
                    NOTIFY_REACHED_USAGE_PERCENT, WEBHOOK_ADDRESS)

if TYPE_CHECKING:
    from app.db.models import User


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


def review():
    now = datetime.utcnow()
    now_ts = now.timestamp()
    with GetDB() as db:
        # Get only active users who need to be reviewed (limited or expired)
        for user in get_users_for_review(db, now_ts):

            try:
                limited = user.data_limit and user.used_traffic >= user.data_limit
                expired = user.expire and user.expire <= now_ts

                if (limited or expired) and user.next_plan is not None:
                    if user.next_plan is not None:

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
                update_user_status(db, user, status)

                report.status_change(username=user.username, status=status,
                                     user=UserResponse.model_validate(user), user_admin=user.admin)

                logger.info(f"User \"{user.username}\" status changed to {status}")
            except ObjectDeletedError:
                logger.warning(f"User object deleted during review, skipping.")
            except SQLAlchemyError:
                logger.exception("Database error while reviewing user")
            except Exception:
                logger.exception("Unknown error while reviewing user")

        # For notification reminders, we still need all active users
        if WEBHOOK_ADDRESS:
            for user in get_users(db, status=UserStatus.active):
                add_notification_reminders(db, user, now)


scheduler.add_job(review, 'interval',
                  seconds=JOB_REVIEW_USERS_INTERVAL,
                  coalesce=True, max_instances=1)
