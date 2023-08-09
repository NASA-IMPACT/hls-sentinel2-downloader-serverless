import dataclasses
import json
from datetime import date, datetime, timezone
from typing import Callable, List
from unittest.mock import patch

import pytest
import responses
from assertpy import assert_that
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from freezegun import freeze_time
from handler import (
    add_scihub_result_to_sqs,
    add_scihub_results_to_db_and_sqs,
    create_scihub_result_from_feed_entry,
    _handler,
    ensure_three_decimal_points_for_milliseconds_and_replace_z,
    filter_scihub_results,
    get_accepted_tile_ids,
    get_fetched_links,
    get_page_for_query_and_total_results,
    get_query_parameters,
    get_scihub_auth,
    update_fetched_links,
    update_last_fetched_link_time,
    update_total_results,
)
from scihub_result import ScihubResult
from sqlalchemy.orm import Session


def test_that_link_fetcher_handler_correctly_loads_allowed_tiles():
    tile_ids = get_accepted_tile_ids()
    assert_that(tile_ids).is_length(18952)
    assert_that(tile_ids).contains("01FBE")
    assert_that(tile_ids).contains("60WWV")


def test_that_link_fetcher_handler_correctly_loads_scihub_credentials(
    mock_scihub_credentials,
):
    auth = get_scihub_auth()
    assert_that(auth[0]).is_equal_to(mock_scihub_credentials["username"])
    assert_that(auth[1]).is_equal_to(mock_scihub_credentials["password"])


def test_that_link_fetcher_handler_generates_correct_query_parameters():
    expected_query_parameters = {
        "q": (
            "(platformname:Sentinel-2) AND (processinglevel:Level-1C) "
            "AND (ingestiondate:[2020-01-01T00:00:00Z TO 2020-01-01T23:59:59Z])"
        ),
        "rows": 100,
        "format": "json",
        "orderby": "ingestiondate desc",
        "start": 0,
    }

    actual_query_parameters = get_query_parameters(start=0, day=date(2020, 1, 1))

    assert_that(actual_query_parameters).is_equal_to(expected_query_parameters)


@responses.activate
def test_that_link_fetcher_handler_generates_a_scihub_result_correctly(
    mock_scihub_response, scihub_result_maker
):
    expected_scihub_result = ScihubResult(
        image_id="422fd86d-7019-47c6-be4f-036fbf5ce874",
        filename="S2B_MSIL1C_20200101T222829_N0208_R129_T51CWM_20200101T230625.SAFE",
        tileid="51CWM",
        size=693056307,
        beginposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        endposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        ingestiondate=datetime(2020, 1, 1, 23, 59, 32, 994000, tzinfo=timezone.utc),
        download_url=(
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce874')/$value"
        ),
    )

    actual_scihub_result = create_scihub_result_from_feed_entry(
        feed_entry=mock_scihub_response["feed"]["entry"][0]
    )

    assert_that(actual_scihub_result).is_equal_to(expected_scihub_result)


def test_that_create_scihub_result_datetime_formatting_creates_iso_formats():
    parsed_datetime = datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:47:22.412Z"
        )
    )
    assert_that(parsed_datetime).is_equal_to(
        datetime(2021, 2, 9, 17, 47, 22, 412000, tzinfo=timezone.utc)
    )

    parsed_datetime = datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:52:37.25Z"
        )
    )
    assert_that(parsed_datetime).is_equal_to(
        datetime(2021, 2, 9, 17, 52, 37, 250000, tzinfo=timezone.utc)
    )

    parsed_datetime = datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:47:19.8Z"
        )
    )
    assert_that(parsed_datetime).is_equal_to(
        datetime(2021, 2, 9, 17, 47, 19, 800000, tzinfo=timezone.utc)
    )

    parsed_datetime = datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:47:19Z"
        )
    )
    assert_that(parsed_datetime).is_equal_to(
        datetime(2021, 2, 9, 17, 47, 19, 000000, tzinfo=timezone.utc)
    )


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results(
    mock_scihub_response, mock_scihub_credentials
):
    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/search?q=(platformname:Sentinel-2) "
            "AND (processinglevel:Level-1C) "
            "AND (ingestiondate:[2020-01-01T00:00:00Z TO 2020-01-01T23:59:59Z])"
            "&rows=100&format=json&orderby=ingestiondate desc&start=0"
        ),
        json=mock_scihub_response,
        status=200,
    )

    scihub_results, total_results = get_page_for_query_and_total_results(
        query_params=get_query_parameters(start=0, day=date(2020, 1, 1)),
        auth=(
            mock_scihub_credentials["username"],
            mock_scihub_credentials["password"],
        ),
    )

    assert_that(scihub_results).is_length(40)
    assert_that(total_results).is_equal_to(6800)


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results_when_no_imagery_left(
    mock_scihub_response, mock_scihub_credentials
):
    resp = mock_scihub_response.copy()
    resp["feed"].pop("entry")

    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/search?q=(platformname:Sentinel-2) "
            "AND (processinglevel:Level-1C) "
            "AND (ingestiondate:[2020-01-01T00:00:00Z TO 2020-01-01T23:59:59Z])"
            "&rows=100&format=json&orderby=ingestiondate desc&start=0"
        ),
        json=resp,
        status=200,
    )

    scihub_results, total_results = get_page_for_query_and_total_results(
        query_params=get_query_parameters(start=0, day=date(2020, 1, 1)),
        auth=(
            mock_scihub_credentials["username"],
            mock_scihub_credentials["password"],
        ),
    )

    assert_that(scihub_results).is_length(0)
    assert_that(total_results).is_equal_to(6800)


def test_that_link_fetcher_handler_correctly_filters_scihub_results(accepted_tile_ids):
    # For testing purposes, we care only about the `tileid` property of each
    # ScihubResult, so we'll just set dummy values for everything, and make
    # copies with meaningful `tileid` values.
    EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
    result = ScihubResult(
        image_id="",
        filename="",
        tileid="",
        size=0,
        beginposition=EPOCH,
        endposition=EPOCH,
        ingestiondate=EPOCH,
        download_url="",
    )

    list_to_filter = [
        dataclasses.replace(result, tileid="51HVC"),
        dataclasses.replace(result, tileid="56HKK"),
        dataclasses.replace(result, tileid="99LOL"),
        dataclasses.replace(result, tileid="20TLT"),
        dataclasses.replace(result, tileid="99IDK"),
        dataclasses.replace(result, tileid="14VLM"),
        dataclasses.replace(result, tileid="01GEM"),
    ]

    expected_results = [
        dataclasses.replace(result, tileid="51HVC"),
        dataclasses.replace(result, tileid="56HKK"),
        dataclasses.replace(result, tileid="20TLT"),
        dataclasses.replace(result, tileid="14VLM"),
        dataclasses.replace(result, tileid="01GEM"),
    ]

    actual_results = filter_scihub_results(list_to_filter, accepted_tile_ids)

    assert_that(actual_results).is_equal_to(expected_results)


def test_that_link_fetcher_handler_correctly_adds_scihub_results_to_db(
    db_session: Session,
    scihub_result_maker: Callable[[int], List[ScihubResult]],
    mock_sqs_queue,
    db_session_context,
):
    scihub_results = scihub_result_maker(10)
    scihub_result_id_base = scihub_results[0].image_id[:-3]
    scihub_result_url_base = scihub_results[0].download_url[:-12]

    with patch("handler.add_scihub_result_to_sqs") as mock_add_to_sqs:
        mock_add_to_sqs.return_value = None
        add_scihub_results_to_db_and_sqs(lambda: db_session, scihub_results)
        mock_add_to_sqs.assert_called()

    granules_in_db = db_session.query(Granule).all()
    assert_that(granules_in_db).is_length(10)

    for idx, granule in enumerate(granules_in_db):
        id_filled = str(idx).zfill(3)
        expected_id = f"{scihub_result_id_base}{id_filled}"
        expected_url = f"{scihub_result_url_base}{id_filled}')/$value"
        granule_id = granule.id
        granule_download_url = granule.download_url
        assert_that(expected_id).is_equal_to(granule_id)
        assert_that(expected_url).is_equal_to(granule_download_url)


def test_that_link_fetcher_handler_correctly_handles_duplicate_db_entry(
    db_session: Session,
    scihub_result_maker: Callable[[int], List[ScihubResult]],
    mock_sqs_queue,
):
    scihub_result = scihub_result_maker(1)[0]
    db_session.add(
        Granule(
            id=scihub_result.image_id,
            filename=scihub_result.filename,
            tileid=scihub_result.tileid,
            size=scihub_result.size,
            beginposition=scihub_result.beginposition,
            endposition=scihub_result.endposition,
            ingestiondate=scihub_result.ingestiondate,
            download_url=scihub_result.download_url,
        )
    )

    # Because of how the db sessions are handled in these unit tests, the rollback
    # call actually undoes the insert that the test setup does, so we're just asserting
    # that rollback is called, not that there is still one entry.
    # That's the best we can do without a full e2e test.
    with patch.object(db_session, "rollback") as rollback:
        add_scihub_results_to_db_and_sqs(lambda: db_session, [scihub_result])
        rollback.assert_called_once()


def test_that_link_fetcher_handler_correctly_adds_scihub_result_to_queue(
    mock_sqs_queue,
    scihub_result_maker: Callable[[int], List[ScihubResult]],
    sqs_client,
):
    scihub_result = scihub_result_maker(1)[0]
    scihub_result_id = scihub_result.image_id
    scihub_result_url = scihub_result.download_url
    scihub_result_filename = scihub_result.filename

    add_scihub_result_to_sqs(scihub_result, sqs_client, mock_sqs_queue.url)

    mock_sqs_queue.load()

    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(1)

    message = mock_sqs_queue.receive_messages(MaxNumberOfMessages=1)[0]
    message_body = json.loads(message.body)
    assert_that(scihub_result_id).is_equal_to(message_body["id"])
    assert_that(scihub_result_url).is_equal_to(message_body["download_url"])
    assert_that(scihub_result_filename).is_equal_to(message_body["filename"])


def test_that_link_fetcher_handler_correctly_retrieves_fetched_links_if_in_db(
    db_session: Session,
):
    expected_fetched_links = 400
    db_session.add(
        GranuleCount(
            date=datetime(2020, 1, 1),
            available_links=0,
            fetched_links=expected_fetched_links,
            last_fetched_time=datetime(2020, 1, 1, 0, 0, 0),
        )
    )
    db_session.commit()

    actual_fetched_links = get_fetched_links(lambda: db_session, datetime(2020, 1, 1))
    assert_that(expected_fetched_links).is_equal_to(actual_fetched_links)


@freeze_time("2020-12-31 10:10:10")
def test_that_link_fetcher_handler_correctly_retrieves_fetched_links_if_not_in_db(
    db_session: Session,
):
    expected_fetched_links = 0
    expected_last_fetched_time = datetime.now()

    actual_fetched_links = get_fetched_links(lambda: db_session, datetime(2020, 12, 31))
    assert_that(expected_fetched_links).is_equal_to(actual_fetched_links)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime(2020, 12, 31))
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
            available_links=250,
            fetched_links=0,
            last_fetched_time=datetime(2020, 1, 1, 0, 0, 0),
        )
    )
    db_session.commit()

    update_total_results(lambda: db_session, datetime(2020, 1, 1), 500)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime(2020, 1, 1))
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
        .filter(Status.key_name == "last_linked_fetched_time")
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
        )
    )
    db_session.commit()

    update_last_fetched_link_time(lambda: db_session)

    last_linked_fetched_time = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")
        .first()
    )

    assert last_linked_fetched_time is not None
    assert_that(last_linked_fetched_time.value).is_equal_to(str(datetime.now()))


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_granule_count(db_session: Session):
    db_session.add(
        GranuleCount(
            date=datetime.now().date(),
            available_links=10000,
            fetched_links=1000,
            last_fetched_time=datetime.now(),
        )
    )
    db_session.commit()

    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime.now().date())
        .first()
    )

    update_fetched_links(lambda: db_session, datetime.now().date(), 1000)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime.now().date())
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
    mock_scihub_credentials,
    db_connection_secret,
    mock_sqs_queue,
):
    _handler({"query_date": "2020-01-01"}, lambda: db_session)

    # Assert all granules present
    granules = db_session.query(Granule).all()
    assert_that(granules).is_length(10)

    query_date = datetime.strptime("2020-01-01", "%Y-%m-%d").date()
    # Assert 2020-01-01 has correct granule count
    granule_count = (
        db_session.query(GranuleCount).filter(GranuleCount.date == query_date).first()
    )
    assert granule_count is not None
    assert_that(granule_count.available_links).is_equal_to(6800)
    assert_that(granule_count.fetched_links).is_equal_to(40)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime.now())

    # Assert status is correct
    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")
        .first()
    )
    assert status is not None
    assert_that(status.value).is_equal_to(str(datetime.now()))

    # Assert queue is populated
    mock_sqs_queue.load()
    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(10)
