import logging
import os

import alembic.command
import alembic.config
from db.session import get_session, get_session_maker
from retry import retry
from sqlalchemy.exc import OperationalError

from .cfnresponse import FAILED as cfnresponse_failed
from .cfnresponse import SUCCESS as cfnresponse_success
from .cfnresponse import send


def log(log_statement: str):
    """
    Gets a Logger for the Lambda function with level logging.INFO and logs
    `log_statement`. This is used multiple times as Alembic takes over the logging
    configuration so we have to re-take control when we want to log
    :param log_statement: str to log
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(log_statement)


@retry(OperationalError, tries=30, delay=10)
def check_rds_connection():
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        db.execute("SELECT * FROM pg_catalog.pg_tables;")


def handler(event, context):
    if event["RequestType"] == "Delete":
        log("Received a Delete Request")
        send(
            event, context, cfnresponse_success, {"Response": "Nothing run on deletes"}
        )
        return

    try:
        log("Checking connection to RDS")
        check_rds_connection()
        log("Connected to RDS")

        log("Running Alembic Migrations")
        alembic_config = alembic.config.Config(os.path.join(".", "alembic.ini"))
        alembic_config.set_main_option("script_location", ".")
        alembic.command.upgrade(alembic_config, "head")
        log("Migrations run successfully")

        send(
            event,
            context,
            cfnresponse_success,
            {"Response": "Migrations run successfully"},
        )
    except Exception as ex:
        log(str(ex))
        send(event, context, cfnresponse_failed, {"Response": str(ex)})
        raise ex
