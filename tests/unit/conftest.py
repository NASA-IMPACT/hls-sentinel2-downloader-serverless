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
