from datetime import datetime
from unittest.mock import patch

import responses
from assertpy import assert_that
from freezegun import freeze_time

from lambdas.link_fetcher.handler import (  # handler,; query_for_imagery,
    get_dates_to_query,
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


# def test_that_link_fetcher_handler_gets_correct_query_results(mock_scihub_response):
#     responses.add(
#         responses.GET,
#         (
#             "https://scihub.copernicus.eu/dhus/search?q=(platformname:Sentinel-2) "
#             "AND (processinglevel:Level-1C) "
#             "AND (ingestiondate:[2020-01-01T00:00:00Z TO 2020-01-01T23:59:59Z])"
#             "&rows=100&format=json&orderby=ingestiondate desc&start=0"
#         ),
#         json=mock_scihub_response,
#         status=200,
#     )

#     scihub_results = query_for_imagery(
#         start_result=0, date=datetime(2020, 1, 1), total_fetched_entries=0
#     )

#     assert_that(scihub_results).is_length(40)
