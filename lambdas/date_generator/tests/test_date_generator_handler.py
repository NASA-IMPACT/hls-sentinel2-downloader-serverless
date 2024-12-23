from datetime import datetime
from itertools import product

from assertpy import assert_that
from freezegun import freeze_time

from handler import get_dates, handler


@freeze_time("2020-01-22")
def test_that_get_dates_returns_correct_dates():
    expected_dates = [
        datetime(2020, 1, i).date().strftime("%Y-%m-%d") for i in range(21, 16, -1)
    ]
    actual_dates = get_dates()
    assert_that(expected_dates).is_equal_to(actual_dates)


@freeze_time("2020-04-22")
def test_that_date_generator_handler_returns_correct_dates():
    expected_dates = [
        datetime(2020, 4, i).date().strftime("%Y-%m-%d") for i in range(21, 16, -1)
    ]
    expected_platforms = ["S2A", "S2B"]
    expected_handler_output = {
        "query_dates_platforms": list(product(expected_dates, expected_platforms))
    }
    actual_handler_output = handler(None, None)
    assert_that(expected_handler_output).is_equal_to(actual_handler_output)
