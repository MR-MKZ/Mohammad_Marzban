from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy.exc import SQLAlchemyError

from app import logger, scheduler
from app.db import GetDB, get_onhold_users_for_review, start_user_expire, update_user_status
from app.models.user import UserResponse, UserStatus
from app.utils import report
from config import JOB_REVIEW_USERS_INTERVAL

if TYPE_CHECKING:
    from app.db.models import User


def review_onhold_users():
    now = datetime.utcnow()
    with GetDB() as db:
        for user in get_onhold_users_for_review(db, now):
            try:
                status = UserStatus.active

                update_user_status(db, user, status)
                start_user_expire(db, user)

                report.status_change(
                    username=user.username,
                    status=status,
                    user=UserResponse.model_validate(user),
                    user_admin=user.admin,
                )

                logger.info(f"User \"{user.username}\" status changed to {status}")
            except ObjectDeletedError:
                logger.warning("User object deleted during on-hold review, skipping.")
            except SQLAlchemyError:
                logger.exception("Database error while reviewing on-hold users")
            except Exception:
                logger.exception("Unknown error while reviewing on-hold users")


scheduler.add_job(
    review_onhold_users,
    "interval",
    seconds=JOB_REVIEW_USERS_INTERVAL,
    coalesce=True,
    max_instances=1,
)
