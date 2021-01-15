import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

import alembic.command
import alembic.config
from lambdas.link_fetcher.handler import ScihubResult

UNIT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def mock_scihub_response():
    with open(
        os.path.join(UNIT_TEST_DIR, "example_scihub_response.json"), "rb"
    ) as json_in:
        return json.load(json_in)


@pytest.fixture
def mock_scihub_checksum_response():
    with open(
        os.path.join(UNIT_TEST_DIR, "example_scihub_checksum_response.json"), "rb"
    ) as json_in:
        return json.load(json_in)


@pytest.fixture
def accepted_tile_ids():
    link_fetcher_dir = UNIT_TEST_DIR.replace(
        os.path.join("tests", "unit"), os.path.join("lambdas", "link_fetcher")
    )
    with open(os.path.join(link_fetcher_dir, "allowed_tiles.txt"), "r") as text_in:
        return [line.strip() for line in text_in]


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def postgres_engine(docker_ip, docker_services):
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

    alembic_config = alembic.config.Config("alembic.ini")
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
    def db_context():
        yield db_session

    yield db_context


def make_scihub_result(idx: int) -> ScihubResult:
    id_filled = str(idx).zfill(3)
    return ScihubResult(
        image_id=f"422fd86d-7019-47c6-be4f-036fbf5ce{id_filled}",
        filename="S2B_MSIL1C20200101T222829_N0208_R129_T51CWM_20200101T230625.SAFE",
        tileid="51CWM",
        size=693056307,
        checksum="66865C45E90E4F5051DE616DEF7B6182",
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


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests", "unit", "docker-compose.yml"
    )
