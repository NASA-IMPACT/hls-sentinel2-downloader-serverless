import dataclasses
from datetime import date, datetime, timezone

import pytest
import responses
from assertpy import assert_that
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from freezegun import freeze_time
from sqlalchemy.orm import Session

from app.common import (
    SearchResult,
)
from app.search_handler import (
    MIN_REMAINING_MILLIS,
    SEARCH_URL,
    _handler,
    create_search_result,
    filter_search_results,
    get_fetched_links,
    get_page_for_query_and_total_results,
    get_query_parameters,
    update_fetched_links,
    update_last_fetched_link_time,
    update_total_results,
)


def test_that_link_fetcher_handler_generates_correct_query_parameters():
    expected_query_parameters = {
        "processingLevel": "S2MSI1C",
        "publishedAfter": "2020-01-01T00:00:00Z",
        "publishedBefore": "2020-01-01T23:59:59Z",
        "startDate": "2019-12-02T00:00:00Z",
        "platform": "S2A",
        "sortParam": "published",
        "sortOrder": "desc",
        "maxRecords": 2000,
        # `start` is 0-based, but `index` is 1-based, so we must add 1 when
        # computing the page number from the `start` index.
        "index": 1,
        "exactCount": 1,
    }

    actual_query_parameters = get_query_parameters(
        start=0, day=date(2020, 1, 1), platform="S2A"
    )

    assert_that(actual_query_parameters).is_equal_to(expected_query_parameters)


@responses.activate
def test_that_link_fetcher_handler_generates_a_search_result_correctly(
    mock_search_response, search_result_maker
):
    expected_search_result = SearchResult(
        image_id="483cab2e-06f1-5944-a06b-ef7026cc6fd0",
        filename="S2A_MSIL1C_20200101T210921_N0208_R057_T04QFJ_20200101T221734.SAFE",
        tileid="04QFJ",
        size=0,
        beginposition=datetime(2020, 1, 1, 21, 9, 21, 24000, tzinfo=timezone.utc),
        endposition=datetime(2020, 1, 1, 21, 9, 21, 24000, tzinfo=timezone.utc),
        ingestiondate=datetime(2020, 1, 1, 23, 39, 20, 33183, tzinfo=timezone.utc),
        download_url=(
            "https://catalogue.dataspace.copernicus.eu/download/483cab2e-06f1-5944-a06b-ef7026cc6fd0"
        ),
    )

    actual_search_result = create_search_result(mock_search_response["features"][0])

    assert_that(actual_search_result).is_equal_to(expected_search_result)


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results(mock_search_response):
    responses.add(
        responses.GET,
        (
            f"{SEARCH_URL}/resto/api/collections/Sentinel2/search.json?processingLevel=S2MSI1C"
            "&publishedAfter=2020-01-01T00:00:00Z"
            "&publishedBefore=2020-01-01T23:59:59Z"
            "&startDate=2019-12-02T00:00:00Z"
            "&platform=S2A"
            "&sortParam=published"
            "&sortOrder=desc"
            "&maxRecords=2000"
            "&index=1"
            "&exactCount=1"
        ),
        json=mock_search_response,
        status=200,
    )

    search_results, total_results = get_page_for_query_and_total_results(
        query_params=get_query_parameters(
            start=0, day=date(2020, 1, 1), platform="S2A"
        ),
    )

    assert_that(search_results).is_length(10)
    assert_that(total_results).is_equal_to(2020)


@responses.activate
def test_that_link_fetcher_handler_defaults_total_results_to_neg1_when_missing(
    mock_search_response,
):
    resp = mock_search_response.copy()
    del resp["properties"]["totalResults"]

    responses.add(
        responses.GET,
        (
            f"{SEARCH_URL}/resto/api/collections/Sentinel2/search.json?processingLevel=S2MSI1C"
            "&publishedAfter=2020-01-01T00:00:00Z"
            "&publishedBefore=2020-01-01T23:59:59Z"
            "&startDate=2019-12-02T00:00:00Z"
            "&platform=S2A"
            "&sortParam=published"
            "&sortOrder=desc"
            "&maxRecords=2000"
            "&index=1"
            "&exactCount=1"
        ),
        json=resp,
        status=200,
    )

    _, total_results = get_page_for_query_and_total_results(
        query_params=get_query_parameters(
            start=0, day=date(2020, 1, 1), platform="S2A"
        ),
    )

    assert total_results == -1


@responses.activate
def test_that_link_fetcher_handler_defaults_total_results_to_neg1_when_null(
    mock_search_response,
):
    resp = mock_search_response.copy()
    resp["properties"]["totalResults"] = None

    responses.add(
        responses.GET,
        (
            f"{SEARCH_URL}/resto/api/collections/Sentinel2/search.json?processingLevel=S2MSI1C"
            "&publishedAfter=2020-01-01T00:00:00Z"
            "&publishedBefore=2020-01-01T23:59:59Z"
            "&startDate=2019-12-02T00:00:00Z"
            "&platform=S2B"
            "&sortParam=published"
            "&sortOrder=desc"
            "&maxRecords=2000"
            "&index=1"
            "&exactCount=1"
        ),
        json=resp,
        status=200,
    )

    _, total_results = get_page_for_query_and_total_results(
        query_params=get_query_parameters(
            start=0, day=date(2020, 1, 1), platform="S2B"
        ),
    )

    assert total_results == -1


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results_when_no_imagery_left(
    mock_search_response,
):
    resp = mock_search_response.copy()
    resp.pop("features")

    responses.add(
        responses.GET,
        (
            f"{SEARCH_URL}/resto/api/collections/Sentinel2/search.json?processingLevel=S2MSI1C"
            "&publishedAfter=2020-01-01T00:00:00Z"
            "&publishedBefore=2020-01-01T23:59:59Z"
            "&startDate=2019-12-02T00:00:00Z"
            "&platform=S2A"
            "&sortParam=published"
            "&sortOrder=desc"
            "&maxRecords=2000"
            "&index=1"
            "&exactCount=1"
        ),
        json=resp,
        status=200,
    )

    search_results, total_results = get_page_for_query_and_total_results(
        query_params=get_query_parameters(
            start=0, day=date(2020, 1, 1), platform="S2A"
        ),
    )

    assert_that(search_results).is_length(0)
    assert_that(total_results).is_equal_to(2020)


def test_that_link_fetcher_handler_correctly_filters_search_results(accepted_tile_ids):
    # For testing purposes, we care only about the `tileid` property of each
    # SearchResult, so we'll just set dummy values for everything, and make
    # copies with meaningful `tileid` values.
    now = datetime.now(timezone.utc)
    result = SearchResult(
        image_id="",
        filename="",
        tileid="",
        size=0,
        beginposition=now,
        endposition=now,
        ingestiondate=now,
        download_url="",
    )

    list_to_filter = [
        dataclasses.replace(result, tileid="51HVC"),  # Accepted tileid
        dataclasses.replace(result, tileid="56HKK"),  # Accepted tileid
        dataclasses.replace(result, tileid="99LOL"),  # Not accepted tileid
        dataclasses.replace(result, tileid="20TLT"),  # Accepted tileid
        dataclasses.replace(result, tileid="99IDK"),  # Not accepted tileid
        dataclasses.replace(result, tileid="14VLM"),  # Accepted tileid
        dataclasses.replace(result, tileid="01GEM"),  # Accepted tileid
    ]

    expected_results = (
        list_to_filter[0],
        list_to_filter[1],
        list_to_filter[3],
        list_to_filter[5],
        list_to_filter[6],
    )

    actual_results = filter_search_results(list_to_filter, accepted_tile_ids)

    assert actual_results == expected_results


def test_that_link_fetcher_handler_correctly_retrieves_fetched_links_if_in_db(
    db_session: Session,
):
    expected_fetched_links = 400
    db_session.add(
        GranuleCount(
            date=datetime(2020, 1, 1),
            platform="S2A",
            available_links=0,
            fetched_links=expected_fetched_links,
            last_fetched_time=datetime(2020, 1, 1, 0, 0, 0),
        )  # type: ignore
    )
    db_session.commit()

    actual_fetched_links = get_fetched_links(
        lambda: db_session, datetime(2020, 1, 1), "S2A"
    )
    assert_that(expected_fetched_links).is_equal_to(actual_fetched_links)


@freeze_time("2020-12-31 10:10:10")
def test_that_link_fetcher_handler_correctly_retrieves_fetched_links_if_not_in_db(
    db_session: Session,
):
    expected_fetched_links = 0
    expected_last_fetched_time = datetime.now()

    actual_fetched_links = get_fetched_links(
        lambda: db_session, datetime(2020, 12, 31), platform="S2B"
    )
    assert_that(expected_fetched_links).is_equal_to(actual_fetched_links)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(
            GranuleCount.date == datetime(2020, 12, 31), GranuleCount.platform == "S2B"
        )  # type: ignore
        .first()
    )

    assert granule_count is not None
    assert_that(expected_last_fetched_time).is_equal_to(granule_count.last_fetched_time)


def test_that_link_fetcher_handler_correctly_updates_available_links_in_db(
    db_session: Session,
):
    expected_available_links = 500
    db_session.add(
        GranuleCount(
            date=datetime(2020, 1, 1),
            platform="S2B",
            available_links=250,
            fetched_links=0,
            last_fetched_time=datetime(2020, 1, 1, 0, 0, 0),
        )  # type: ignore
    )
    db_session.commit()

    update_total_results(lambda: db_session, datetime(2020, 1, 1), "S2B", 500)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(
            GranuleCount.date == datetime(2020, 1, 1), GranuleCount.platform == "S2B"
        )  # type: ignore
        .first()
    )

    assert granule_count is not None
    actual_available_links = granule_count.available_links
    assert_that(expected_available_links).is_equal_to(actual_available_links)


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_last_linked_fetched_time_when_not_present(
    db_session: Session,
):
    update_last_fetched_link_time(lambda: db_session)

    last_linked_fetched_time = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")  # type: ignore
        .first()
    )

    assert last_linked_fetched_time is not None
    assert_that(last_linked_fetched_time.value).is_equal_to(str(datetime.now()))


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_last_linked_fetched_time(
    db_session: Session,
):
    db_session.add(
        Status(
            key_name="last_linked_fetched_time",
            value=str(datetime(year=2000, month=12, day=1, hour=1, minute=1, second=2)),
        )  # type: ignore
    )
    db_session.commit()

    update_last_fetched_link_time(lambda: db_session)

    last_linked_fetched_time = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")  # type: ignore
        .first()
    )

    assert last_linked_fetched_time is not None
    assert_that(last_linked_fetched_time.value).is_equal_to(str(datetime.now()))


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_granule_count(db_session: Session):
    db_session.add(
        GranuleCount(
            date=datetime.now().date(),
            platform="S2B",
            available_links=10000,
            fetched_links=1000,
            last_fetched_time=datetime.now(),
        )  # type: ignore
    )
    db_session.commit()

    granule_count = (
        db_session.query(GranuleCount)
        .filter(
            GranuleCount.date == datetime.now().date(), GranuleCount.platform == "S2B"
        )  # type: ignore
        .first()
    )

    update_fetched_links(lambda: db_session, datetime.now().date(), "S2B", 1000)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(
            GranuleCount.date == datetime.now().date(), GranuleCount.platform == "S2B"
        )  # type: ignore
        .first()
    )

    assert granule_count is not None
    assert_that(granule_count.fetched_links).is_equal_to(2000)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime.now())


@responses.activate
@freeze_time("2020-01-01")
@pytest.mark.usefixtures("generate_mock_responses_for_one_day")
def test_that_link_fetcher_handler_correctly_functions(
    db_session: Session,
    db_session_context,
    db_connection_secret,
    mock_sqs_queue,
):
    class MockContext:
        def get_remaining_time_in_millis(self) -> int:
            return MIN_REMAINING_MILLIS

    result = _handler(
        {"query_date_platform": ("2020-01-01", "S2A")},
        MockContext(),
        lambda: db_session,
    )

    assert result == {"query_date_platform": ("2020-01-01", "S2A"), "completed": True}

    # Assert all filtered granules present
    granules = db_session.query(Granule).all()
    assert_that(granules).is_length(5)  # 5 of 10 are filtered out

    query_date = datetime.strptime("2020-01-01", "%Y-%m-%d").date()
    # Assert 2020-01-01 has correct granule count
    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == query_date)  # type: ignore
        .first()
    )
    assert granule_count is not None
    assert_that(granule_count.available_links).is_equal_to(2020)
    assert_that(granule_count.fetched_links).is_equal_to(10)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime.now())

    # Assert status is correct
    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")  # type: ignore
        .first()
    )
    assert status is not None
    assert_that(status.value).is_equal_to(str(datetime.now()))

    # Assert queue is populated
    mock_sqs_queue.load()
    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(5)


@responses.activate
@freeze_time("2020-01-01")
@pytest.mark.usefixtures("generate_mock_responses_for_one_day")
def test_that_link_fetcher_handler_bails_early(
    db_session: Session,
    db_session_context,
    db_connection_secret,
    mock_sqs_queue,
):
    class MockContext:
        def get_remaining_time_in_millis(self) -> int:
            return MIN_REMAINING_MILLIS - 1

    result = _handler(
        {"query_date_platform": ("2020-01-01", "S2A")},
        MockContext(),
        lambda: db_session,
    )

    # Assert that we bailed early
    assert result == {"query_date_platform": ("2020-01-01", "S2A"), "completed": False}

    # Assert all filtered granules present
    granules = db_session.query(Granule).all()
    assert_that(granules).is_length(4)  # 1 of the first 5 is filtered out

    query_date = datetime.strptime("2020-01-01", "%Y-%m-%d").date()
    # Assert 2020-01-01 has correct granule count
    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == query_date, GranuleCount.platform == "S2A")  # type: ignore
        .first()
    )
    assert granule_count is not None
    assert_that(granule_count.available_links).is_equal_to(2020)
    assert_that(granule_count.fetched_links).is_equal_to(5)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime.now())

    # Assert status is correct
    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")  # type: ignore
        .first()
    )
    assert status is not None
    assert_that(status.value).is_equal_to(str(datetime.now()))

    # Assert queue is populated
    mock_sqs_queue.load()
    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(4)
