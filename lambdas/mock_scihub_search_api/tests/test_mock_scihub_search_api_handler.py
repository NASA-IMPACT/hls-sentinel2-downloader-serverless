import json

from assertpy import assert_that
from freezegun import freeze_time

from handler import handler


@freeze_time("2020-02-10")
def test_that_yesterday_index_1_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"publishedAfter": "2020-02-09", "index": "1"}}, None
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["properties"]["totalResults"]).is_equal_to(6786)
    assert_that(body["properties"]["startIndex"]).is_equal_to(1)
    assert_that(body["features"]).is_length(100)


@freeze_time("2020-02-10")
def test_that_yesterday_index_101_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"publishedAfter": "2020-02-09", "index": "101"}},
        None,
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["properties"]["totalResults"]).is_equal_to(6786)
    assert_that(body["properties"]["startIndex"]).is_equal_to(101)
    assert_that(body["features"]).is_length(100)


@freeze_time("2020-02-10")
def test_that_yesterday_any_start_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"publishedAfter": "2020-02-09", "index": "201"}},
        None,
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["properties"]["totalResults"]).is_equal_to(6786)
    assert_that(body["features"]).is_empty()


@freeze_time("2020-02-10")
def test_that_any_day_any_start_returns_correct_response():
    resp = handler(
        {"queryStringParameters": {"publishedAfter": "", "start": "19000"}}, None
    )
    assert_that(resp["statusCode"]).is_equal_to("200")
    body = json.loads(resp["body"])
    assert_that(body["properties"]["totalResults"]).is_equal_to(6786)
    assert_that(body["features"]).is_empty()
