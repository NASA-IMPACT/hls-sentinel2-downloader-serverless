import re
from datetime import datetime, timezone
from unittest.mock import patch

import responses
from assertpy import assert_that
from freezegun import freeze_time

from db.models.granule import Granule
from lambdas.link_fetcher.handler import (
    ScihubResult,
    add_scihub_results_to_db,
    create_scihub_result_from_feed_entry,
    filter_scihub_results,
    get_dates_to_query,
    get_image_checksum,
    get_page_for_query_and_total_results,
    get_query_parameters,
)


@freeze_time("2020-12-31")
def test_that_link_fetcher_handler_gets_correct_dates_to_query():
    expected_dates = [
        datetime(2020, 12, 31),
        datetime(2020, 12, 30),
        datetime(2020, 12, 29),
        datetime(2020, 12, 28),
    ]

    actual_dates = get_dates_to_query(
        last_date=datetime(2020, 12, 31), number_of_dates_to_query=4
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

    actual_query_parameters = get_query_parameters(start=0, day=datetime(2020, 1, 1))

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
        (
            "https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('422fd86d-7019-47c6-be4f-036fbf5ce874')/"
        )
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
        mock_scihub_response["feed"]["entry"][0]
    )

    assert_that(actual_scihub_result).is_equal_to(expected_scihub_result)


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results(
    mock_scihub_response, mock_scihub_checksum_response
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
        get_query_parameters(start=0, day=datetime(2020, 1, 1))
    )

    assert_that(scihub_results).is_length(40)
    assert_that(total_results).is_equal_to(6800)


@responses.activate
def test_that_link_fetcher_handler_gets_correct_query_results_when_no_imagery_left(
    mock_scihub_response, mock_scihub_checksum_response
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
        get_query_parameters(start=0, day=datetime(2020, 1, 1))
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

    with patch("lambdas.link_fetcher.handler.get_session", db_session_context):
        add_scihub_results_to_db(scihub_results)
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

    with patch("lambdas.link_fetcher.handler.get_session", db_session_context):
        add_scihub_results_to_db([scihub_result])
        granules_in_db = db_session.query(Granule).all()
        assert_that(granules_in_db).is_length(1)
