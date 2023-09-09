import os
from typing import Callable, Iterable, NoReturn, Union

import boto3
import polling2
import pytest
from db.session import get_session, get_session_maker
from dotenv import load_dotenv
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3ServiceResource
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient
from mypy_boto3_sqs import SQSClient


EMPTY_TABLES_QUERY = (
    "DELETE FROM GRANULE; DELETE FROM GRANULE_COUNT; DELETE FROM STATUS;"
)


@pytest.fixture(scope="session", autouse=True)
def load_env() -> None:
    load_dotenv()


@pytest.fixture(scope="session")
def identifier() -> str:
    return os.environ["IDENTIFIER"].replace("/", "")


@pytest.fixture
def ssm_client() -> Iterable[SSMClient]:
    yield boto3.client("ssm")


@pytest.fixture
def s3_resource() -> Iterable[S3ServiceResource]:
    yield boto3.resource("s3")


@pytest.fixture
def step_function_client() -> Iterable[SFNClient]:
    yield boto3.client("stepfunctions")


@pytest.fixture
def lambda_client() -> Iterable[LambdaClient]:
    yield boto3.client("lambda")


@pytest.fixture
def ssm_parameter(
    ssm_client: SSMClient, identifier: str
) -> Callable[[str], Union[str, NoReturn]]:
    def get_value(name: str) -> Union[str, NoReturn]:
        qname = f"/integration_tests/{identifier}/{name}"
        result = ssm_client.get_parameter(Name=qname)
        value = result["Parameter"].get("Value")
        assert value is not None, f"No such SSM parameter: {qname}"

        return value

    return get_value


@pytest.fixture
def db_setup(monkeypatch, ssm_parameter: Callable[[str], str]):
    db_connection_secret_arn = ssm_parameter("downloader_rds_secret_arn")
    monkeypatch.setenv("DB_CONNECTION_SECRET_ARN", db_connection_secret_arn)
    session_maker = get_session_maker()

    with get_session(session_maker) as db:
        db.execute(EMPTY_TABLES_QUERY)
        db.commit()
        yield
        db.execute(EMPTY_TABLES_QUERY)
        db.commit()


@pytest.fixture
def step_function_arn(ssm_parameter: Callable[[str], str]):
    return ssm_parameter("link_fetcher_step_function_arn")


@pytest.fixture
def queue_url(ssm_parameter: Callable[[str], str]):
    return ssm_parameter("to_download_queue_url")


@pytest.fixture
def mock_scihub_api_url(ssm_parameter: Callable[[str], str]):
    return ssm_parameter("mock_scihub_url")


@pytest.fixture
def downloader_arn(ssm_parameter: Callable[[str], str]):
    return ssm_parameter("downloader_arn")


@pytest.fixture
def upload_bucket_name(ssm_parameter: Callable[[str], str]):
    return ssm_parameter("upload_bucket_name")


@pytest.fixture
def upload_bucket(s3_resource: S3ServiceResource, upload_bucket_name: str):
    bucket = s3_resource.Bucket(upload_bucket_name)
    bucket.objects.all().delete()

    yield bucket

    bucket.objects.all().delete()


def purge(sqs: SQSClient, url: str):
    try:
        sqs.purge_queue(QueueUrl=url)
        return True
    except sqs.exceptions.PurgeQueueInProgress:
        return False


def approximate_messages_is_zero(sqs: SQSClient, url: str):
    result = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(result["Attributes"]["ApproximateNumberOfMessages"]) == 0


@pytest.fixture
def sqs_client(queue_url: str) -> Iterable[SQSClient]:
    client = boto3.client("sqs")

    print("Purging SQS Queue before test")
    polling2.poll(purge, args=(client, queue_url), step=10, timeout=120)
    print("Ensuring SQS Queue empty")
    polling2.poll(
        approximate_messages_is_zero, args=(client, queue_url), step=10, timeout=300
    )
    print("SQS Queue empty")

    yield client

    print("Purging SQS Queue after test")
    polling2.poll(purge, args=(client, queue_url), step=10, timeout=120)
    print("Ensuring SQS Queue empty")
    polling2.poll(
        approximate_messages_is_zero, args=(client, queue_url), step=10, timeout=300
    )
    print("SQS Queue empty")
