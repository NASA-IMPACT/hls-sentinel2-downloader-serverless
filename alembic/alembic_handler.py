import logging
import os

import cfnresponse

import alembic.command
import alembic.config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):

    if event["RequestType"] == "Delete":
        # cfnresponse.send(
        #     event, context, cfnresponse.SUCCESS, {"Response": "Nothing run on deletes"}
        # )
        return

    try:
        logger.info("Running Alembic Migrations")

        alembic_config = alembic.config.Config(os.path.join(".", "alembic.ini"))
        alembic_config.set_main_option("script_location", ".")
        alembic.command.upgrade(alembic_config, "head")

        logger.info("Migrations run successfully")

        # cfnresponse.send(
        #     event,
        #     context,
        #     cfnresponse.SUCCESS,
        #     {"Response": "Migrations run successfully"},
        # )
    except Exception as ex:
        logger.info(ex)
        # cfnresponse.send(event, context, cfnresponse.FAILED, {})
        raise ex
