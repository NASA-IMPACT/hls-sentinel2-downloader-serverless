import json
import os

import pytest

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


@pytest.fixture(scope="session")
def postgres_container(docker_ip, docker_services):
    port = docker_services.port_for("postgres", 5432)
    url = f"http://{docker_ip}:{port}"
    return url


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests", "unit", "docker-compose.yml"
    )
