import json
import re
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import responses
from assertpy import assert_that
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from freezegun import freeze_time

from handler import (
    add_scihub_results_to_db,
    add_scihub_results_to_sqs,
    create_scihub_result_from_feed_entry,
    ensure_three_decimal_points_for_milliseconds_and_replace_z,
    filter_scihub_results,
    get_accepted_tile_ids,
    get_available_and_fetched_links,
    get_dates_to_query,
    get_image_checksum,
    get_page_for_query_and_total_results,
    get_query_parameters,
    get_scihub_auth,
    handler,
    update_fetched_links,
    update_last_fetched_link_time,
    update_total_results,
)
from scihub_result import ScihubResult


def test_that_link_fetcher_handler_correctly_loads_allowed_tiles():
    tile_ids = get_accepted_tile_ids()
    assert_that(tile_ids).is_length(18501)
    assert_that(tile_ids[0]).is_equal_to("01FBE")
    assert_that(tile_ids[18500]).is_equal_to("60WWV")


def test_that_link_fetcher_handler_correctly_loads_scihub_credentials(
    mock_scihub_credentials,
):
    auth = get_scihub_auth()
    assert_that(auth[0]).is_equal_to(mock_scihub_credentials["username"])
    assert_that(auth[1]).is_equal_to(mock_scihub_credentials["password"])


@freeze_time("2020-12-31")
def test_that_link_fetcher_handler_gets_correct_dates_to_query():
    expected_dates = [
        date(2020, 12, 31),
        date(2020, 12, 30),
        date(2020, 12, 29),
        date(2020, 12, 28),
    ]

    actual_dates = get_dates_to_query(
        last_date=date(2020, 12, 31), number_of_dates_to_query=4
    )

    assert_that(actual_dates).is_equal_to(expected_dates)


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
def test_that_link_fetcher_handler_gets_product_checksum_correctly(
    mock_scihub_checksum_response,
):
    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce874')/"
            "?$format=json&$select=Checksum"
        ),
        json=mock_scihub_checksum_response,
        status=200,
    )

    expected_checksum = mock_scihub_checksum_response["d"]["Checksum"]["Value"]

    actual_checksum = get_image_checksum(
        product_url=(
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce874')/"
        ),
        auth=("blah", "blah"),
    )

    assert_that(actual_checksum).is_equal_to(expected_checksum)


@responses.activate
def test_that_link_fetcher_handler_generates_a_scihub_result_correctly(
    mock_scihub_response, mock_scihub_checksum_response, scihub_result_maker
):
    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce874')/"
            "?$format=json&$select=Checksum"
        ),
        json=mock_scihub_checksum_response,
        status=200,
    )

    expected_scihub_result = ScihubResult(
        image_id="422fd86d-7019-47c6-be4f-036fbf5ce874",
        filename="S2B_MSIL1C_20200101T222829_N0208_R129_T51CWM_20200101T230625.SAFE",
        tileid="51CWM",
        size=693056307,
        checksum="66865C45E90E4F5051DE616DEF7B6182",
        beginposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        endposition=datetime(2020, 1, 1, 22, 28, 29, 24000, tzinfo=timezone.utc),
        ingestiondate=datetime(2020, 1, 1, 23, 59, 32, 994000, tzinfo=timezone.utc),
        download_url=(
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce874')/$value"
        ),
    )

    actual_scihub_result = create_scihub_result_from_feed_entry(
        feed_entry=mock_scihub_response["feed"]["entry"][0], auth=("blah", "blah")
    )

    assert_that(actual_scihub_result).is_equal_to(expected_scihub_result)


def test_that_create_scihub_result_datetime_formatting_creates_iso_formats():
    datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:47:22.412Z"
        )
    )
    datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:52:37.25Z"
        )
    )
    datetime.fromisoformat(
        ensure_three_decimal_points_for_milliseconds_and_replace_z(
            "2021-02-09T17:47:19.8Z"
        )
    )


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results(
    mock_scihub_response, mock_scihub_checksum_response, mock_scihub_credentials
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
    responses.add(
        responses.GET,
        re.compile(r"https://scihub.copernicus.eu/dhus/odata/v1/Products\('*."),
        json=mock_scihub_checksum_response,
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
    mock_scihub_response, mock_scihub_checksum_response, mock_scihub_credentials
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
    responses.add(
        responses.GET,
        re.compile(r"https://scihub.copernicus.eu/dhus/odata/v1/Products\('*."),
        json=mock_scihub_checksum_response,
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
    list_to_filter = [
        ScihubResult(tileid="51HVC"),
        ScihubResult(tileid="56HKK"),
        ScihubResult(tileid="99LOL"),
        ScihubResult(tileid="20TLT"),
        ScihubResult(tileid="99IDK"),
        ScihubResult(tileid="14VLM"),
        ScihubResult(tileid="01GEM"),
    ]

    expected_results = [
        ScihubResult(tileid="51HVC"),
        ScihubResult(tileid="56HKK"),
        ScihubResult(tileid="20TLT"),
        ScihubResult(tileid="14VLM"),
        ScihubResult(tileid="01GEM"),
    ]

    actual_results = filter_scihub_results(list_to_filter, accepted_tile_ids)

    assert_that(actual_results).is_equal_to(expected_results)


def test_that_link_fetcher_handler_correctly_adds_scihub_results_to_db(
    db_session, db_session_context, scihub_result_maker
):
    scihub_results = scihub_result_maker(10)
    scihub_result_id_base = scihub_results[0]["image_id"][:-3]
    scihub_result_url_base = scihub_results[0]["download_url"][:-12]

    with patch("handler.get_session", db_session_context):
        add_scihub_results_to_db(None, scihub_results)
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
    db_session, db_session_context, scihub_result_maker
):
    scihub_result = scihub_result_maker(1)[0]
    db_session.add(
        Granule(
            id=scihub_result["image_id"],
            filename=scihub_result["filename"],
            tileid=scihub_result["tileid"],
            size=scihub_result["size"],
            checksum=scihub_result["checksum"],
            beginposition=scihub_result["beginposition"],
            endposition=scihub_result["endposition"],
            ingestiondate=scihub_result["ingestiondate"],
            download_url=scihub_result["download_url"],
        )
    )
    db_session.commit()

    with patch("handler.get_session", db_session_context):
        add_scihub_results_to_db(None, [scihub_result])
        granules_in_db = db_session.query(Granule).all()
        assert_that(granules_in_db).is_length(1)


def test_that_link_fetcher_handler_correctly_adds_scihub_results_to_queue(
    mock_sqs_queue, scihub_result_maker
):
    scihub_results = scihub_result_maker(10)
    scihub_result_id_base = scihub_results[0]["image_id"][:-3]
    scihub_result_url_base = scihub_results[0]["download_url"][:-12]

    add_scihub_results_to_sqs(scihub_results)

    mock_sqs_queue.load()
    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(10)
    for idx, message in enumerate(
        mock_sqs_queue.receive_messages(MaxNumberOfMessages=10)
    ):
        id_filled = str(idx).zfill(3)
        expected_id = f"{scihub_result_id_base}{id_filled}"
        expected_url = f"{scihub_result_url_base}{id_filled}')/$value"
        message_body = json.loads(message.body)
        assert_that(expected_id).is_equal_to(message_body["id"])
        assert_that(expected_url).is_equal_to(message_body["download_url"])


def test_that_link_fetcher_handler_correctly_retrieves_available_and_fetched_links_if_in_db(  # noqa
    db_session, db_session_context
):
    expected_available_links = 1000
    expected_fetched_links = 400
    db_session.add(
        GranuleCount(
            date=datetime(2020, 1, 1),
            available_links=expected_available_links,
            fetched_links=expected_fetched_links,
            last_fetched_time=datetime(2020, 1, 1, 0, 0, 0),
        )
    )
    db_session.commit()

    with patch("handler.get_session", db_session_context):
        actual_available_links, actual_fetched_links = get_available_and_fetched_links(
            None, datetime(2020, 1, 1)
        )
        assert_that(expected_available_links).is_equal_to(actual_available_links)
        assert_that(expected_fetched_links).is_equal_to(actual_fetched_links)


@freeze_time("2020-12-31 10:10:10")
def test_that_link_fetcher_handler_correctly_retrieves_available_and_fetched_links_if_not_in_db(  # noqa
    db_session, db_session_context
):
    expected_available_links = 0
    expected_fetched_links = 0
    expected_last_fetched_time = datetime.now()

    with patch("handler.get_session", db_session_context):
        actual_available_links, actual_fetched_links = get_available_and_fetched_links(
            None, datetime(2020, 12, 31)
        )
        assert_that(expected_available_links).is_equal_to(actual_available_links)
        assert_that(expected_fetched_links).is_equal_to(actual_fetched_links)

    actual_last_fetched_time = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime(2020, 12, 31))
        .first()
        .last_fetched_time
    )

    assert_that(expected_last_fetched_time).is_equal_to(actual_last_fetched_time)


def test_that_link_fetcher_handler_correctly_updates_available_links_in_db(
    db_session, db_session_context
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

    with patch("handler.get_session", db_session_context):
        update_total_results(None, datetime(2020, 1, 1), 500)

    actual_available_links = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime(2020, 1, 1))
        .first()
        .available_links
    )

    assert_that(expected_available_links).is_equal_to(actual_available_links)


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_last_linked_fetched_time_when_not_present(  # noqa
    db_session, db_session_context
):
    with patch("handler.get_session", db_session_context):
        update_last_fetched_link_time(None)

    last_linked_fetched_time = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")
        .first()
        .value
    )

    assert_that(last_linked_fetched_time).is_equal_to(str(datetime.now()))


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_last_linked_fetched_time(
    db_session, db_session_context
):
    db_session.add(
        Status(
            key_name="last_linked_fetched_time",
            value=str(datetime(year=2000, month=12, day=1, hour=1, minute=1, second=2)),
        )
    )
    db_session.commit()

    with patch("handler.get_session", db_session_context):
        update_last_fetched_link_time(None)

    last_linked_fetched_time = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")
        .first()
        .value
    )

    assert_that(last_linked_fetched_time).is_equal_to(str(datetime.now()))


@freeze_time("2021-01-01 00:00:01")
def test_that_link_fetcher_handler_correctly_updates_granule_count(
    db_session, db_session_context
):
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

    with patch("handler.get_session", db_session_context):
        update_fetched_links(None, datetime.now().date(), 1000)

    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime.now().date())
        .first()
    )

    assert_that(granule_count.fetched_links).is_equal_to(2000)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime.now())


@responses.activate
@freeze_time("2020-01-01")
@pytest.mark.usefixtures("generate_mock_responses_for_multiple_days")
def test_that_link_fetcher_handler_correctly_functions_for_multiple_days(
    db_session,
    db_session_context,
    mock_scihub_credentials,
    db_connection_secret,
    mock_sqs_queue,
):
    datetime_now = datetime.now()
    datetime_day_behind_now = datetime_now - timedelta(days=1)

    with patch("handler.get_session", db_session_context):
        with patch("handler.get_dates_to_query") as mock_get_dates:
            # Only run for 2 days as generating test data for 21 is cumbersome
            mock_get_dates.return_value = [
                datetime_now.date(),
                datetime_day_behind_now.date(),
            ]
            handler(None, None)

    # Assert all granules present
    granules = db_session.query(Granule).all()
    assert_that(granules).is_length(20)

    # Assert 2020-01-01 has correct granule count
    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime_now.date())
        .first()
    )
    assert_that(granule_count.available_links).is_equal_to(6800)
    assert_that(granule_count.fetched_links).is_equal_to(40)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime_now)

    # Assert 2019-12-31 has correct granule count
    granule_count = (
        db_session.query(GranuleCount)
        .filter(GranuleCount.date == datetime_day_behind_now.date())
        .first()
    )
    assert_that(granule_count.available_links).is_equal_to(6800)
    assert_that(granule_count.fetched_links).is_equal_to(40)
    assert_that(granule_count.last_fetched_time).is_equal_to(datetime_now)

    # Assert status is correct
    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_linked_fetched_time")
        .first()
    )
    assert_that(status.value).is_equal_to(str(datetime_now))

    # Assert queue is populated
    mock_sqs_queue.load()
    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(20)
