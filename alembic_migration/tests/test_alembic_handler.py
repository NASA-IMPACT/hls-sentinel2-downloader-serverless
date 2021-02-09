from unittest.mock import call, patch

import pytest
from assertpy import assert_that

from alembic_migration.alembic_handler import handler

LIST_TABLES_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
"""


@patch("alembic_migration.alembic_handler.log")
def test_that_alembic_handler_correctly_migrates_db(mock_log, db_session):
    result = db_session.execute(LIST_TABLES_SQL).fetchall()
    assert_that(result).is_length(0)

    with patch("alembic_migration.alembic_handler.send") as mock_send:
        mock_send.return_value = True
        event = {"RequestType": "Create"}
        handler(event, None)
        mock_send.assert_called_with(
            event, None, "SUCCESS", {"Response": "Migrations run successfully"}
        )

    mock_log.assert_has_calls(
        [
            call("Checking connection to RDS"),
            call("Connected to RDS"),
            call("Running Alembic Migrations"),
            call("Migrations run successfully"),
        ]
    )

    result = db_session.execute(LIST_TABLES_SQL).fetchall()
    assert_that(result).is_length(4)
    assert_that(result[0].values()[0]).is_equal_to("alembic_version")
    assert_that(result[1].values()[0]).is_equal_to("granule")
    assert_that(result[2].values()[0]).is_equal_to("granule_count")
    assert_that(result[3].values()[0]).is_equal_to("status")


@patch("alembic_migration.alembic_handler.log")
def test_that_alembic_handler_correctly_does_nothing_on_deletes(mock_log, db_session):
    with patch("alembic_migration.alembic_handler.send") as mock_send:
        mock_send.return_value = True
        event = {"RequestType": "Delete"}
        handler(event, None)
        mock_send.assert_called_once_with(
            event, None, "SUCCESS", {"Response": "Nothing run on deletes"}
        )

    mock_log.assert_has_calls([call("Received a Delete Request")])

    # We could check that the tables aren't created, but this would require us to drop
    # the tables in the db_session fixture cleanup - This is only an issue here
    # as the alembic scripts' commits are external to our db_sessions


@patch("alembic_migration.alembic_handler.check_rds_connection")
@patch("alembic_migration.alembic_handler.log")
def test_that_alembic_handler_handles_exception_correctly(
    mock_log, mock_check_rds_connection
):
    failure_exception = Exception("A Failure")
    mock_check_rds_connection.side_effect = failure_exception

    with patch("alembic_migration.alembic_handler.send") as mock_send:
        mock_send.return_value = True
        event = {"RequestType": "Create"}

        with pytest.raises(Exception):
            handler(event, None)

        mock_send.assert_called_once_with(
            event, None, "FAILED", {"Response": str(failure_exception)}
        )

    mock_log.assert_has_calls(
        [call("Checking connection to RDS"), call(str(failure_exception))]
    )
