import json
import os
import re
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import List

import alembic.command
import alembic.config
import boto3
import pytest
import responses
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_secretsmanager, mock_sqs
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from scihub_result import ScihubResult

UNIT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def mock_scihub_response():
    with open(
        os.path.join(UNIT_TEST_DIR, "example_scihub_response.json"), "rb"
    ) as json_in:
        return json.load(json_in)


@pytest.fixture
def accepted_tile_ids():
    link_fetcher_dir = UNIT_TEST_DIR.replace("tests", "")
    with open(os.path.join(link_fetcher_dir, "allowed_tiles.txt"), "r") as text_in:
        return [line.strip() for line in text_in]


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def postgres_engine(docker_ip, docker_services, db_connection_secret):
    db_url = url.URL(
        "postgresql",
        username=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        host="localhost",
        database=os.environ["PG_DB"],
    )
    pg_engine = create_engine(db_url)
    docker_services.wait_until_responsive(
        timeout=15.0, pause=1, check=lambda: check_pg_status(pg_engine)
    )

    alembic_root = UNIT_TEST_DIR.replace(
        "lambdas/link_fetcher/tests", "alembic_migration"
    )
    alembic_config = alembic.config.Config(os.path.join(alembic_root, "alembic.ini"))
    alembic_config.set_main_option("script_location", alembic_root)
    alembic.command.upgrade(alembic_config, "head")

    return pg_engine


@pytest.fixture
def db_session(postgres_engine):
    connection = postgres_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def db_session_context(db_session):
    @contextmanager
    def db_context(session_maker):
        yield db_session

    yield db_context


def make_scihub_result(idx: int) -> ScihubResult:
    id_filled = str(idx).zfill(3)
    return ScihubResult(
        image_id=f"422fd86d-7019-47c6-be4f-036fbf5ce{id_filled}",
        filename="S2B_MSIL1C20200101T222829_N0208_R129_T51CWM_20200101T230625.SAFE",
        tileid="51CWM",
        size=693056307,
        beginposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        endposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        ingestiondate=datetime(2020, 1, 1, 23, 59, 32, 994000, tzinfo=timezone.utc),
        download_url=(
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce"
            f"{id_filled}')/$value"
        ),
    )


@pytest.fixture
def scihub_result_maker():
    def make_scihub_results(number_of_results: int) -> List[ScihubResult]:
        return [make_scihub_result(idx) for idx in range(0, number_of_results)]

    return make_scihub_results


@pytest.fixture(autouse=True)
def aws_credentials(monkeysession):
    monkeysession.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeysession.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeysession.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeysession.setenv("AWS_SESSION_TOKEN", "testing")
    monkeysession.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def sqs_resource():
    with mock_sqs():
        yield boto3.resource("sqs")


@pytest.fixture
def mock_sqs_queue(sqs_resource, monkeysession):
    queue = sqs_resource.create_queue(QueueName="mock-queue")
    monkeysession.setenv("TO_DOWNLOAD_SQS_QUEUE_URL", queue.url)
    return queue


@pytest.fixture(scope="session")
def secrets_manager_client():
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="session")
def db_connection_secret(secrets_manager_client, monkeysession):
    arn = secrets_manager_client.create_secret(
        Name="db-connection",
        SecretString=json.dumps(
            {
                "username": os.environ["PG_USER"],
                "password": os.environ["PG_PASSWORD"],
                "host": "localhost",
                "dbname": os.environ["PG_DB"],
            }
        ),
    )["ARN"]
    monkeysession.setenv("DB_CONNECTION_SECRET_ARN", arn)


@pytest.fixture(scope="session")
def mock_scihub_credentials(secrets_manager_client, monkeysession):
    secret = {"username": "test-username", "password": "test-password"}
    secrets_manager_client.create_secret(
        Name="hls-s2-downloader-serverless/test/scihub-credentials",
        SecretString=json.dumps(secret),
    )
    monkeysession.setenv("STAGE", "test")
    return secret


@pytest.fixture
def generate_mock_responses_for_one_day(
    mock_scihub_response,
):
    scihub_query_base_fmt = (
        "https://scihub.copernicus.eu/dhus/search?q=(platformname:Sentinel-2) "
        "AND (processinglevel:Level-1C) "
        "AND (ingestiondate:[{0}T00:00:00Z TO {0}T23:59:59Z])"
        "&rows=100&format=json&orderby=ingestiondate desc&start={1}"
    )

    # Generate base for response
    scihub_response_2020 = deepcopy(mock_scihub_response)
    total_entries = len(scihub_response_2020["feed"]["entry"])

    # Give each entry a unique ID for tests
    for idx in range(0, total_entries):
        scihub_response_2020["feed"]["entry"][idx]["id"] = str(idx + 1)

    # Generate 3 responses per year, 2x 20 entry results and 1 empty result
    scihub_response_2020_first_20 = deepcopy(scihub_response_2020)
    scihub_response_2020_first_20["feed"]["entry"] = scihub_response_2020["feed"][
        "entry"
    ][:20]
    scihub_response_2020_next_20 = deepcopy(scihub_response_2020)
    scihub_response_2020_next_20["feed"]["entry"] = scihub_response_2020["feed"][
        "entry"
    ][20:]
    scihub_response_2020_empty = deepcopy(scihub_response_2020)
    scihub_response_2020_empty["feed"].pop("entry")

    # Create responses for sentinel query based on year and start point
    responses.add(
        responses.GET,
        scihub_query_base_fmt.format("2020-01-01", 0),
        json=scihub_response_2020_first_20,
        status=200,
    )
    responses.add(
        responses.GET,
        scihub_query_base_fmt.format("2020-01-01", 20),
        json=scihub_response_2020_next_20,
        status=200,
    )
    responses.add(
        responses.GET,
        scihub_query_base_fmt.format("2020-01-01", 40),
        json=scihub_response_2020_empty,
        status=200,
    )


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(UNIT_TEST_DIR, "docker-compose.yml")


@pytest.fixture(scope="session")
def monkeysession(request):
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()
