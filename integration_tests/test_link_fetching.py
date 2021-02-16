import json

import polling2
from assertpy import assert_that
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status


def check_execution_succeeded(step_function_client, execution_arn):
    execution_status = step_function_client.describe_execution(
        executionArn=execution_arn
    )["status"]
    return True if execution_status == "SUCCEEDED" else False


def check_sqs_message_count(sqs_client, queue_url):
    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(queue_attributes["Attributes"]["ApproximateNumberOfMessages"]) == 68


def test_that_link_fetching_invocation_executes_correctly(
    db_connection, step_function_arn, step_function_client, sqs_client, queue_url
):
    execution_arn = step_function_client.start_execution(
        stateMachineArn=step_function_arn, input=json.dumps({})
    )["executionArn"]

    polling2.poll(
        lambda: check_execution_succeeded(step_function_client, execution_arn),
        step=10,
        timeout=180,
    )

    granules = db_connection.query(Granule).all()
    assert_that(granules).is_length(68)

    granule_counts = db_connection.query(GranuleCount).all()
    assert_that(granule_counts).is_length(21)

    statuses = db_connection.query(Status).all()
    assert_that(statuses).is_length(1)

    polling2.poll(
        lambda: check_sqs_message_count(sqs_client, queue_url), step=10, timeout=180
    )
