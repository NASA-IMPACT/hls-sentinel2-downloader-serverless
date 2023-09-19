import base64
import io
import json
import zipfile

from assertpy import assert_that
from handler import handler


def test_that_handler_returns_correct_checksum_response_for_known_product_id(
    scihub_response_mock_image_checksum,
):
    request = {
        "pathParameters": {"product": "Products(integration-test-id)"},
    }
    resp = handler(request, None)
    assert_that(json.loads(resp["body"])).is_equal_to(
        scihub_response_mock_image_checksum
    )
    assert_that(resp["statusCode"]).is_equal_to(200)


def test_that_handler_returns_correct_image_response_for_known_product_id():
    request = {
        "pathParameters": {"product": "Products(integration-test-id)/$value"},
    }
    expected_headers = {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": 'attachment; filename="blah.SAFE"',
    }
    resp = handler(request, None)
    assert_that(resp["statusCode"]).is_equal_to(200)
    assert_that(resp["isBase64Encoded"]).is_true()
    assert_that(resp["headers"]).is_equal_to(expected_headers)
    resp_body = resp["body"]
    resp_body_bytes = base64.b64decode(resp_body)

    with zipfile.ZipFile(io.BytesIO(resp_body_bytes)) as zip_in:
        files = zip_in.namelist()
        assert_that(files).is_length(1)
        assert_that(files[0]).is_equal_to("test_file.txt")


def test_that_handler_returns_not_found_if_unknown_product_id():
    request = {
        "pathParameters": {"product": "Products('fake-test-id')"},
    }
    resp = handler(request, None)
    assert_that("body" in resp).is_false()
    assert_that(resp["statusCode"]).is_equal_to(404)


def test_that_handler_returns_not_found_if_invalid_request():
    resp = handler({}, None)
    assert_that("body" in resp).is_false()
    assert_that(resp["statusCode"]).is_equal_to(404)
