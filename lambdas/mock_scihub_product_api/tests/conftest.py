import json
import os

import pytest

UNIT_TEST_DATA_DIR = os.path.dirname(os.path.abspath(__file__)).replace(
    "tests", "scihub_responses"
)


@pytest.fixture
def scihub_response_mock_image_checksum():
    with open(
        os.path.join(UNIT_TEST_DATA_DIR, "scihub_response_mock_image_checksum.json"),
        "rb",
    ) as file_in:
        yield json.load(file_in)
