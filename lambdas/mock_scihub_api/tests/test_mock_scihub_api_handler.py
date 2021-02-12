import json

from assertpy import assert_that
from freezegun import freeze_time

from handler import handler


@freeze_time("2020-02-10")
def test_that_yesterday_start_0_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"q": "a query with 2020-02-09", "start": "0"}}, None
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["feed"]["opensearch:totalResults"]).is_equal_to("6800")
    assert_that(body["feed"]["opensearch:startIndex"]).is_equal_to("0")
    assert_that(body["feed"]["entry"]).is_length(100)


@freeze_time("2020-02-10")
def test_that_yesterday_start_100_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"q": "a query with 2020-02-09", "start": "100"}},
        None,
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["feed"]["opensearch:totalResults"]).is_equal_to("6800")
    assert_that(body["feed"]["opensearch:startIndex"]).is_equal_to("100")
    assert_that(body["feed"]["entry"]).is_length(100)


@freeze_time("2020-02-10")
def test_that_yesterday_any_start_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"q": "a query with 2020-02-09", "start": "200"}},
        None,
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["feed"]["opensearch:totalResults"]).is_equal_to("6800")
    assert_that(body["feed"]).does_not_contain("entry")


@freeze_time("2020-02-10")
def test_that_any_day_any_start_returns_correct_response():
    resp = handler({"queryStringParameters": {"q": "a query", "start": "19000"}}, None)
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["feed"]["opensearch:totalResults"]).is_equal_to("1000")
    assert_that(body["feed"]).does_not_contain("entry")
