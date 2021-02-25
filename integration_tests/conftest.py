import os

import boto3
import polling2
import pytest
from db.session import get_session, get_session_maker

IDENTIFIER = os.environ["IDENTIFIER"].replace("/", "")
EMPTY_TABLES_QUERY = (
    "DELETE FROM GRANULE; DELETE FROM GRANULE_COUNT; DELETE FROM STATUS;"
)


@pytest.fixture
def ssm_client():
    yield boto3.client("ssm")


@pytest.fixture
def step_function_client():
    yield boto3.client("stepfunctions")


@pytest.fixture
def db_setup(monkeypatch, ssm_client):
    db_connection_secret_arn = ssm_client.get_parameter(
        Name=f"/integration_tests/{IDENTIFIER}/downloader_rds_secret_arn"
    )["Parameter"]["Value"]
    monkeypatch.setenv("DB_CONNECTION_SECRET_ARN", db_connection_secret_arn)
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        db.execute(EMPTY_TABLES_QUERY)
        db.commit()
        yield
        db.execute(EMPTY_TABLES_QUERY)
        db.commit()


@pytest.fixture
def step_function_arn(ssm_client):
    link_fetcher_step_function_secret_arn = ssm_client.get_parameter(
        Name=f"/integration_tests/{IDENTIFIER}/link_fetcher_step_function_arn"
    )["Parameter"]["Value"]
    return link_fetcher_step_function_secret_arn


@pytest.fixture
def queue_url(ssm_client):
    to_download_queue_url = ssm_client.get_parameter(
        Name=f"/integration_tests/{IDENTIFIER}/to_download_queue_url"
    )["Parameter"]["Value"]
    return to_download_queue_url


def purge(client, url):
    try:
        client.purge_queue(QueueUrl=url)
        return True
    except client.exceptions.PurgeQueueInProgress:
        return False


def approximate_messages_is_zero(client, url):
    queue_attributes = client.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(queue_attributes["Attributes"]["ApproximateNumberOfMessages"]) == 0


@pytest.fixture
def sqs_client(queue_url):
    sqs_client = boto3.client("sqs")
    print("Purging SQS Queue before test")
    polling2.poll(lambda: purge(sqs_client, queue_url), step=10, timeout=120)
    print("Ensuring SQS Queue empty")
    polling2.poll(
        lambda: approximate_messages_is_zero(sqs_client, queue_url),
        step=10,
        timeout=300,
    )
    print("SQS Queue empty")
    yield sqs_client
    print("Purging SQS Queue after test")
    polling2.poll(lambda: purge(sqs_client, queue_url), step=10, timeout=120)
    print("Ensuring SQS Queue empty")
    polling2.poll(
        lambda: approximate_messages_is_zero(sqs_client, queue_url),
        step=10,
        timeout=300,
    )
    print("SQS Queue empty")
