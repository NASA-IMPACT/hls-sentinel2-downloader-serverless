import json
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, url
from sqlalchemy.exc import OperationalError

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
def postgres_engine_and_url(docker_ip, docker_services):
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
    return (pg_engine, db_url)


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests", "unit", "docker-compose.yml"
    )
