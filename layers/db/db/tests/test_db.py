import os

import pytest
from assertpy import assert_that

from ..models.granule import Granule
from ..models.granule_count import GranuleCount
from ..models.status import Status
from ..session import _get_url, get_session, get_session_maker


@pytest.mark.usefixtures("db_connection_secret")
def test_that_db_correctly_gets_db_connection_details():
    url = _get_url()
    assert_that(url.drivername).is_equal_to("postgresql")
    assert_that(url.host).is_equal_to("localhost")
    assert_that(url.username).is_equal_to(os.environ["PG_USER"])
    assert_that(url.password).is_equal_to(os.environ["PG_PASSWORD"])
    assert_that(url.database).is_equal_to(os.environ["PG_DB"])


@pytest.mark.usefixtures("db_connection_secret")
@pytest.mark.usefixtures("db_session")
def test_that_db_can_create_successful_connection_with_granule():
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        granules = db.query(Granule).all()
        assert_that(granules).is_length(0)


@pytest.mark.usefixtures("db_connection_secret")
@pytest.mark.usefixtures("db_session")
def test_that_db_can_create_successful_connection_with_granule_count():
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        granule_counts = db.query(GranuleCount).all()
        assert_that(granule_counts).is_length(0)


@pytest.mark.usefixtures("db_connection_secret")
@pytest.mark.usefixtures("db_session")
def test_that_db_can_create_successful_connection_with_status():
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        statuses = db.query(Status).all()
        assert_that(statuses).is_length(0)
