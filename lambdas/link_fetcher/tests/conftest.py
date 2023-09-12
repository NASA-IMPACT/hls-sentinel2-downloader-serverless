import json
import os
import pathlib
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Callable, Sequence, Set, cast

import alembic.command
import alembic.config
import boto3
import pytest
import responses
from _pytest.monkeypatch import MonkeyPatch
from handler import SEARCH_URL, SearchResult
from moto import mock_secretsmanager, mock_sqs
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Transaction, url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

UNIT_TEST_DIR = pathlib.Path(__file__).parent


@pytest.fixture
def mock_search_response():
    return json.loads((UNIT_TEST_DIR / "example_search_response.json").read_text())


@pytest.fixture
def accepted_tile_ids() -> Set[str]:
    with open(UNIT_TEST_DIR.parent / "allowed_tiles.txt") as lines:
        return set(map(str.strip, lines))


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def postgres_engine(docker_ip, docker_services, db_connection_secret):
    db_url = url.URL.create(
        "postgresql",
        username=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        host="localhost",
        database=os.environ["PG_DB"],
    )
    pg_engine = cast(Engine, create_engine(db_url))
    docker_services.wait_until_responsive(
        timeout=15.0, pause=1, check=lambda: check_pg_status(pg_engine)
    )

    alembic_root = UNIT_TEST_DIR.parent.parent.parent / "alembic_migration"
    alembic_config = alembic.config.Config(str(alembic_root / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(alembic_root))
    alembic.command.upgrade(alembic_config, "head")

    return pg_engine


@pytest.fixture
def db_session(postgres_engine: Engine):
    with postgres_engine.connect() as connection:
        with cast(Transaction, connection.begin()) as transaction:
            with Session(bind=connection) as session:
                yield session
            transaction.rollback()


@pytest.fixture
def db_session_context(db_session):
    @contextmanager
    def db_context(session_maker):
        yield db_session

    yield db_context


def make_search_result(idx: int) -> SearchResult:
    id_filled = str(idx).zfill(3)

    return SearchResult(
        image_id=f"422fd86d-7019-47c6-be4f-036fbf5ce{id_filled}",
        filename="S2B_MSIL1C20200101T222829_N0208_R129_T51CWM_20200101T230625.SAFE",
        tileid="51CWM",
        size=693056307,
        beginposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        endposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        ingestiondate=datetime(2020, 1, 1, 23, 59, 32, 994000, tzinfo=timezone.utc),
        download_url=(
            "https://zipper.creodias.eu/download/"
            f"bde39034-06c2-5927-ba1c-4960a201f{id_filled}"
        ),
    )


@pytest.fixture
def search_result_maker() -> Callable[[int], Sequence[SearchResult]]:
    def make_search_results(number_of_results: int) -> Sequence[SearchResult]:
        return tuple(map(make_search_result, range(number_of_results)))

    return make_search_results


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
def sqs_client():
    with mock_sqs():
        yield boto3.client("sqs")


@pytest.fixture
def mock_sqs_queue(sqs_resource, monkeysession, sqs_client):
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


@pytest.fixture
def generate_mock_responses_for_one_day(mock_search_response):
    search_query_fmt = (
        f"{SEARCH_URL}?processingLevel=S2MSI1C"
        "&publishedAfter={0}T00:00:00Z"
        "&publishedBefore={0}T23:59:59Z"
        "&startDate=2019-12-02T00:00:00Z"
        "&sortParam=published"
        "&sortOrder=desc"
        "&maxRecords=100"
        "&index={1}"
    )

    # Generate base for response
    search_response_2020 = deepcopy(mock_search_response)

    # Generate 3 responses per year, 2 x 5 entry results and 1 empty result
    search_response_2020_page1 = deepcopy(search_response_2020)
    search_response_2020_page1["features"] = search_response_2020["features"][:5]
    search_response_2020_page2 = deepcopy(search_response_2020)
    search_response_2020_page2["features"] = search_response_2020["features"][5:]
    search_response_2020_empty = deepcopy(search_response_2020)
    search_response_2020_empty["features"] = []

    # Create responses for sentinel query based on year and start point
    responses.add(
        responses.GET,
        search_query_fmt.format("2020-01-01", 1),
        json=search_response_2020_page1,
        status=200,
    )
    responses.add(
        responses.GET,
        search_query_fmt.format("2020-01-01", 6),
        json=search_response_2020_page2,
        status=200,
    )
    responses.add(
        responses.GET,
        search_query_fmt.format("2020-01-01", 11),
        json=search_response_2020_empty,
        status=200,
    )


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return UNIT_TEST_DIR / "docker-compose.yml"


@pytest.fixture(scope="session")
def monkeysession(request):
    with MonkeyPatch().context() as mp:
        yield mp
