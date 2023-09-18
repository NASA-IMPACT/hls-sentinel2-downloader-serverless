import json
from datetime import datetime

import polling2
from assertpy import assert_that
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from db.session import get_session, get_session_maker


def check_execution_succeeded(step_function_client, execution_arn):
    return (
        step_function_client.describe_execution(executionArn=execution_arn)["status"]
        == "SUCCEEDED"
    )


def check_sqs_message_count(sqs_client, queue_url, count):
    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(queue_attributes["Attributes"]["ApproximateNumberOfMessages"]) == count


def test_that_link_fetching_invocation_executes_correctly(
    db_setup, step_function_arn, step_function_client, sqs_client, queue_url
):
    execution_arn = step_function_client.start_execution(
        stateMachineArn=step_function_arn, input=json.dumps({})
    )["executionArn"]

    polling2.poll(
        check_execution_succeeded,
        args=(step_function_client, execution_arn),
        step=5,
        timeout=120,
    )

    session_maker = get_session_maker()

    with get_session(session_maker) as db:
        granules = db.query(Granule).all()
        assert_that(granules).is_length(78)

        granule_counts = db.query(GranuleCount).all()
        assert_that(granule_counts).is_length(5)

        statuses = db.query(Status).all()
        assert_that(statuses).is_length(1)

    polling2.poll(
        check_sqs_message_count, args=(sqs_client, queue_url, 78), step=5, timeout=120
    )


def test_that_link_fetching_invocation_executes_correctly_when_a_duplicate_granule_present(  # Noqa
    db_setup, step_function_arn, step_function_client, sqs_client, queue_url
):
    session_maker = get_session_maker()

    with get_session(session_maker) as db_insert_duplicate:
        # We're adding a duplicate of the 4th entry in
        # lambdas/mock_scihub_api/scihub_responses/scihub_response_start_0_yesterday
        # Using its ID
        db_insert_duplicate.add(
            Granule(
                id="483cab2e-06f1-5944-a06b-ef7026cc6fd0",
                filename="A filename",
                tileid="TS101",
                size=100,
                beginposition=datetime.now(),
                endposition=datetime.now(),
                ingestiondate=datetime.now(),
                download_url="A download url",
            )
        )
        db_insert_duplicate.commit()
        db_insert_duplicate.close()

    execution_arn = step_function_client.start_execution(
        stateMachineArn=step_function_arn, input=json.dumps({})
    )["executionArn"]

    polling2.poll(
        check_execution_succeeded,
        args=(step_function_client, execution_arn),
        step=5,
        timeout=120,
    )

    session_maker = get_session_maker()

    with get_session(session_maker) as db:
        granules = db.query(Granule).all()
        assert_that(granules).is_length(78)

        # Assert that the original granule we added is still there
        granule_we_inserted = (
            db.query(Granule)
            .filter(Granule.id == "483cab2e-06f1-5944-a06b-ef7026cc6fd0")
            .first()
        )
        assert_that(granule_we_inserted.tileid).is_equal_to("TS101")
        assert_that(granule_we_inserted.download_url).is_equal_to("A download url")

        granule_counts = db.query(GranuleCount).all()
        assert_that(granule_counts).is_length(5)

        statuses = db.query(Status).all()
        assert_that(statuses).is_length(1)

    polling2.poll(
        check_sqs_message_count, args=(sqs_client, queue_url, 77), step=5, timeout=120
    )
