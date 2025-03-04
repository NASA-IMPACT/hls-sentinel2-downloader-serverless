import json
import re
from datetime import datetime
from unittest import mock

import pytest
import responses
from assertpy import assert_that
from botocore.client import ClientError
from db.models.granule import Granule
from db.models.status import Status
from freezegun import freeze_time
from responses import matchers

from exceptions import (
    ChecksumRetrievalException,
    FailedToDownloadFileException,
    FailedToRetrieveGranuleException,
    FailedToUpdateGranuleDownloadFinishException,
    FailedToUploadFileException,
    GranuleAlreadyDownloadedException,
    GranuleNotFoundException,
    RetryLimitReachedException,
)
from handler import (
    download_file,
    generate_aws_checksum,
    get_download_url,
    get_granule,
    get_image_checksum,
    handler,
    increase_retry_count,
    update_last_file_downloaded_time,
)

download_url = "http://zipper.dataspace.copernicus.eu/odata/v1/Products(test-id)/$value"
checksum_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Id eq 'test-id'"


def test_that_get_download_url_returns_correct_url():
    expected = download_url
    actual = get_download_url("test-id")
    assert_that(actual).is_equal_to(expected)


@freeze_time("2020-01-01 01:00:00")
def test_that_get_granule_returns_correct_granule(
    db_session,
):
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
        )
    )
    db_session.commit()

    granule = get_granule("test-id")
    assert_that(granule.tileid).is_equal_to("NM901")
    assert_that(granule.size).is_equal_to(100)
    assert_that(granule.downloaded).is_false()


def test_that_get_granule_throws_exception_when_no_granule_found():
    with pytest.raises(GranuleNotFoundException) as ex:
        get_granule("test-id")
    assert_that(str(ex.value)).is_equal_to("Granule with id: test-id not found")


def test_that_get_granule_throws_exception_when_already_downloaded(db_session):
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=True,
        )
    )
    db_session.commit()

    with pytest.raises(GranuleAlreadyDownloadedException):
        get_granule("test-id")


def test_that_increase_retry_correctly_updates_value(db_session):
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
            download_retries=5,
        )
    )
    db_session.commit()

    increase_retry_count("test-id")

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.download_retries).is_equal_to(6)


@responses.activate
def test_that_get_image_checksum_returns_correct_value(example_checksum_response):
    responses.add(
        responses.GET,
        checksum_url,
        json=example_checksum_response,
        status=200,
    )
    expected_checksum_value = example_checksum_response["value"][0]["Checksum"][0][
        "Value"
    ]
    checksum_value = get_image_checksum("test-id")
    assert_that(checksum_value).is_equal_to(expected_checksum_value)


@responses.activate
def test_exception_thrown_if_error_in_retrieving_image_checksum():
    responses.add(
        responses.GET,
        checksum_url,
        status=404,
    )
    with pytest.raises(ChecksumRetrievalException) as ex:
        get_image_checksum("test-id")
    assert_that(str(ex.value)).matches(
        (
            "There was an error retrieving the Checksum for Granule with id: test-id"
            r".* Not Found .*"
        )
    )


def test_that_generate_aws_checksum_correctly_creates_a_base64_version():
    expected_checksum = "bpy4ihvr6Io1Q8gCfL+71g=="
    actual_checksum = generate_aws_checksum("6E9CB88A1BEBE88A3543C8027CBFBBD6")
    assert_that(expected_checksum).is_equal_to(actual_checksum)


@responses.activate
def test_that_download_file_correctly_raises_exception_if_request_fails(
    db_session, mock_get_copernicus_token
):
    download_url = (
        "https://zipper.dataspace.copernicus.eu/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=404,
        match=[matchers.header_matcher({"Authorization": "Bearer token"})],
    )

    with pytest.raises(FailedToDownloadFileException) as ex:
        download_file(
            "ACHECKSUM",
            "test-id",
            "test-filename.SAFE",
            download_url,
        )
    assert_that(str(ex.value)).is_equal_to(
        (
            "Requests exception thrown downloading granule with "
            f"download_url: {download_url}, exception was: 404 Client Error: "
            "Not Found"
            " for url: https://zipper.dataspace.copernicus.eu/odata/v1/"
            "Products('test-id')"
            "/$value"
        )
    )


@responses.activate
def test_that_download_file_correctly_raises_exception_if_s3_upload_fails(
    db_session, mock_s3_bucket, mock_get_copernicus_token
):
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
        match=[matchers.header_matcher({"Authorization": "Bearer token"})],
    )

    class FakeClient:
        def put_object(self, **args):
            raise ClientError(
                {"Error": {"Code": "500", "Message": "Something Broke"}},
                "Something Broke",
            )

    with mock.patch("handler.get_s3_client") as patch_get_s3_client:
        patch_get_s3_client.return_value = FakeClient()
        with pytest.raises(FailedToUploadFileException) as ex:
            download_file(
                "36F3AB53F6D2D9592CF50CE4682FF7EA",
                "test-id",
                "test-filename.SAFE",
                download_url,
            )
        assert_that(str(ex.value)).is_equal_to(
            (
                "Boto3 Client Error raised when uploading file: test-filename.SAFE"
                " for granule with id: test-id, error was: An error occurred (500) "
                "when calling the Something Broke operation: Something Broke"
            )
        )


@responses.activate
def test_that_download_file_correctly_raises_exception_if_db_update_fails(
    db_session,
    mock_s3_bucket,
    fake_db_session_that_fails,
    mock_get_copernicus_token,
):
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
            download_retries=5,
        )
    )
    db_session.commit()

    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
    )
    with pytest.raises(FailedToUpdateGranuleDownloadFinishException) as ex:
        with mock.patch("handler.get_session", fake_db_session_that_fails):
            download_file(
                "36F3AB53F6D2D9592CF50CE4682FF7EA",
                "test-id",
                "test-filename.SAFE",
                download_url,
            )
    assert_that(str(ex.value)).is_equal_to(
        (
            "SQLAlchemy Exception raised when updating download finish for granule"
            " with id: test-id, exception was: An Exception"
        )
    )


@responses.activate
@freeze_time("2020-01-01 00:00:00")
@mock.patch("handler.generate_aws_checksum")
def test_that_download_file_correctly_uploads_file_to_s3_and_updates_db(
    patched_generate_aws_checksum,
    db_session,
    fake_safe_file_contents,
    mock_s3_bucket,
    mock_get_copernicus_token,
    example_checksum_response,
):
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
            download_retries=5,
        )
    )
    db_session.commit()
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        status=200,
        stream=True,
    )
    patched_generate_aws_checksum.return_value = "an-aws-checksum"

    download_file("ACHECKSUM", "test-id", "test-filename.SAFE", download_url)

    patched_generate_aws_checksum.assert_called_once_with("ACHECKSUM")

    bucket_objects = list(mock_s3_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to("test-filename.zip")
    bucket_object_content = bucket_objects[0].get()["Body"].read().decode("utf-8")
    assert_that(bucket_object_content).contains("THIS IS A FAKE SAFE FILE")

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to("ACHECKSUM")


@freeze_time("2020-01-01 10:10:10")
def test_that_update_last_file_downloaded_time_correctly_updates_the_db_if_not_there(
    db_session,
):
    update_last_file_downloaded_time()
    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_file_downloaded_time")
        .first()
    )
    assert_that(status.value).is_equal_to(str(datetime.now()))


@freeze_time("2020-01-01 10:10:10")
def test_that_update_last_file_downloaded_time_correctly_updates_the_db_if_already_there(  # Noqa
    db_session,
):
    db_session.add(
        Status(
            key_name="last_file_downloaded_time", value=datetime(2020, 1, 1, 0, 0, 0)
        )
    )
    db_session.commit()
    update_last_file_downloaded_time()
    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_file_downloaded_time")
        .first()
    )
    assert_that(status.value).is_equal_to(str(datetime.now()))


def test_that_update_last_file_downloaded_time_fails_gracefully(
    db_session, fake_db_session_that_fails
):
    with mock.patch("handler.LOGGER.error") as mock_logger:
        with mock.patch("handler.get_session", fake_db_session_that_fails):
            update_last_file_downloaded_time()
    mock_logger.assert_called_once_with(
        (
            "Failed to update Status with key_name: last_file_downloaded_time, "
            "exception was: An Exception"
        )
    )


def test_that_handler_correctly_logs_and_returns_if_no_granule_found():
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": "test-url",
                    }
                )
            }
        ]
    }

    with mock.patch("handler.LOGGER.error") as patched_logger:
        handler(sqs_message, None)
        patched_logger.assert_called_once_with("Granule with id: test-id not found")


def test_that_handler_correctly_logs_and_returns_if_error_getting_granule(
    fake_db_session_that_fails,
):
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": "test-url",
                    }
                )
            }
        ]
    }

    expected_error_message = (
        "Failed to retrieve granule with id: test-id, exception was: An Exception"
    )

    with mock.patch("handler.get_session", fake_db_session_that_fails):
        with pytest.raises(
            FailedToRetrieveGranuleException, match=expected_error_message
        ):
            handler(sqs_message, None)


def test_that_handler_correctly_logs_and_returns_if_already_downloaded(
    db_session,
):
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": "test-url",
                    }
                )
            }
        ]
    }
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=True,
        )
    )
    db_session.commit()

    with mock.patch("handler.LOGGER.info") as patched_logger:
        handler(sqs_message, None)
        patched_logger.assert_called_with(
            "Granule with id: test-id has already been downloaded"
        )


def test_that_handler_correctly_logs_and_errors_if_retry_limit_reached(
    db_session,
):
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": "test-url",
                    }
                )
            }
        ]
    }
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
            download_retries=11,
        )
    )
    db_session.commit()

    with pytest.raises(
        RetryLimitReachedException,
        match="Granule with id: test-id has reached its retry limit",
    ):
        handler(sqs_message, None)


@responses.activate
@mock.patch("handler.increase_retry_count")
def test_that_handler_correctly_logs_and_errors_if_get_image_checksum_fails(
    mock_increase_retry_count, db_session
):
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": "test-url",
                    }
                )
            }
        ]
    }
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
        )
    )
    db_session.commit()

    with mock.patch("handler.LOGGER.error") as patched_logger:
        with pytest.raises(ChecksumRetrievalException):
            handler(sqs_message, None)
            patched_logger.assert_called_once_with(
                (
                    "There was an error retrieving the Checksum for Granule with id:"
                    " test-id. An exception"
                )
            )
    mock_increase_retry_count.assert_called_once()


@responses.activate
@mock.patch("handler.get_image_checksum")
@mock.patch("handler.increase_retry_count")
def test_that_handler_correctly_logs_and_errors_if_image_fails_to_download(
    mock_increase_retry_count,
    mock_get_image_checksum,
    db_session,
    mock_get_copernicus_token,
):
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=404,
    )
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": (download_url),
                    }
                )
            }
        ]
    }
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
        )
    )
    db_session.commit()

    expected_error_message = re.escape(
        "Requests exception thrown downloading granule with download_url:"
        f" {download_url}, exception was: 404 Client Error: Not Found for url:"
        f" {download_url}"
    )

    mock_get_image_checksum.return_value = "test-checksum"

    with pytest.raises(FailedToDownloadFileException, match=expected_error_message):
        handler(sqs_message, None)

    mock_increase_retry_count.assert_called_once()


@responses.activate
@mock.patch("handler.get_image_checksum")
@mock.patch("handler.increase_retry_count")
def test_that_handler_correctly_logs_and_errors_if_image_fails_to_upload(
    mock_increase_retry_count,
    mock_get_image_checksum,
    db_session,
    mock_s3_bucket,
    mock_get_copernicus_token,
):
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
    )
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename.SAFE",
                        "download_url": (download_url),
                    }
                )
            }
        ]
    }
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
        )
    )
    db_session.commit()

    class FakeClient:
        def put_object(self, **args):
            raise ClientError(
                {"Error": {"Code": "500", "Message": "Something Broke"}},
                "Something Broke",
            )

    expected_error_message = re.escape(
        "Boto3 Client Error raised when uploading file: test-filename.SAFE for granule"
        " with id: test-id, error was: An error occurred (500) when calling the "
        "Something Broke operation: Something Broke"
    )

    mock_get_image_checksum.return_value = "36F3AB53F6D2D9592CF50CE4682FF7EA"

    with mock.patch("handler.get_s3_client", FakeClient):
        with pytest.raises(FailedToUploadFileException, match=expected_error_message):
            handler(sqs_message, None)

    mock_increase_retry_count.assert_called_once()


@responses.activate
@mock.patch("handler.get_image_checksum")
@mock.patch("handler.get_granule")
@mock.patch("handler.increase_retry_count")
def test_that_handler_correctly_logs_and_errors_if_update_download_finish_fails(
    mock_increase_retry_count,
    mock_get_granule,
    mock_get_image_checksum,
    db_session,
    mock_s3_bucket,
    fake_db_session_that_fails,
    mock_get_copernicus_token,
):
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
    )
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename.SAFE",
                        "download_url": (download_url),
                    }
                )
            }
        ]
    }
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url="blah",
            downloaded=False,
        )
    )
    db_session.commit()

    class MockGranule:
        def __init__(self):
            self.downloaded = False
            self.download_retries = 0
            self.beginposition = datetime.now()

    mock_get_granule.return_value = MockGranule()
    mock_get_image_checksum.return_value = "36F3AB53F6D2D9592CF50CE4682FF7EA"

    expected_error_message = (
        "SQLAlchemy Exception raised when updating download finish for"
        " granule with id: test-id, exception was: An Exception"
    )

    with mock.patch("handler.get_session", fake_db_session_that_fails):
        with pytest.raises(
            FailedToUpdateGranuleDownloadFinishException, match=expected_error_message
        ):
            handler(sqs_message, None)

    mock_increase_retry_count.assert_called_once()
    mock_get_image_checksum.assert_called_once()


@responses.activate
@freeze_time("2020-02-02 00:00:00")
@mock.patch("handler.LOGGER.info")
def test_that_handler_correctly_downloads_file_and_updates_granule(
    patched_logger,
    db_session,
    fake_safe_file_contents,
    mock_s3_bucket,
    mock_get_copernicus_token,
    example_checksum_response,
):
    """Happy path case for link fetching via scheduled search

    In this pathway we do NOT know the download checksum because the search API doesn't
    include it.
    """
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": download_url,
                    }
                )
            }
        ]
    }
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        stream=True,
        status=200,
    )
    checksum_value = "36F3AB53F6D2D9592CF50CE4682FF7EA"
    responses.add(
        responses.GET,
        checksum_url,
        json={
            "value": [
                {
                    "Checksum": [
                        {
                            "Value": checksum_value,
                            "Algorithm": "MD5",
                        }
                    ]
                }
            ]
        },
        status=200,
    )
    db_session.add(
        Granule(
            id="test-id",
            filename="test-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url=download_url,
            downloaded=False,
        )
    )
    db_session.commit()

    handler(sqs_message, None)

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to(checksum_value)

    bucket_objects = list(mock_s3_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to("test-filename.zip")
    bucket_object_content = bucket_objects[0].get()["Body"].read().decode("utf-8")
    assert_that(bucket_object_content).contains("THIS IS A FAKE SAFE FILE")

    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_file_downloaded_time")
        .first()
    )
    assert_that(status.value).is_equal_to(str(datetime.now()))

    patched_logger.assert_has_calls(
        [
            mock.call("Received event to download image: test-filename"),
            mock.call("Successfully downloaded image: test-filename"),
        ]
    )
    responses.assert_call_count(checksum_url.replace(" ", "%20"), 1)


@responses.activate
@freeze_time("2020-02-02 00:00:00")
@mock.patch("handler.LOGGER.info")
@mock.patch("handler.get_image_checksum")
def test_that_handler_doesnt_get_checksum_if_provided_in_message(
    patched_get_image_checksum,
    patched_logger,
    db_session,
    fake_safe_file_contents,
    mock_s3_bucket,
    mock_get_copernicus_token,
    example_checksum_response,
):
    """Ensure we don't try to fetch the checksum on first attempt if provided in message

    The subscription API includes the checksum in the payload they send us, so we don't
    need to ask ESA for the checksum again.
    """
    checksum_value = "36F3AB53F6D2D9592CF50CE4682FF7EA"
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": download_url,
                        "checksum": checksum_value,
                    }
                )
            }
        ]
    }
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        stream=True,
        status=200,
    )
    db_session.add(
        Granule(
            id="test-id",
            filename="test-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url=download_url,
            downloaded=False,
            checksum=checksum_value,
        )
    )
    db_session.commit()

    handler(sqs_message, None)

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to(checksum_value)

    bucket_objects = list(mock_s3_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to("test-filename.zip")
    bucket_object_content = bucket_objects[0].get()["Body"].read().decode("utf-8")
    assert_that(bucket_object_content).contains("THIS IS A FAKE SAFE FILE")

    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_file_downloaded_time")
        .first()
    )
    assert_that(status.value).is_equal_to(str(datetime.now()))

    patched_logger.assert_has_calls(
        [
            mock.call("Received event to download image: test-filename"),
            mock.call("Successfully downloaded image: test-filename"),
        ]
    )
    patched_get_image_checksum.assert_not_called()


@responses.activate
@freeze_time("2020-02-02 00:00:00")
@mock.patch("handler.LOGGER.info")
def test_that_handler_fetches_checksum_if_retry_count_greater_than_zero(
    patched_logger,
    db_session,
    fake_safe_file_contents,
    mock_s3_bucket,
    mock_get_copernicus_token,
    example_checksum_response,
):
    """Ensure the download handler fetches the MD5 checksum if download is retried

    We've observed that ESA sometimes provides an inaccurate checksum, so we want to
    ensure that our downloader doesn't continually use an incorrect checksum and tries
    to refetch it on a repeat attempt.
    """
    checksum_value = example_checksum_response["value"][0]["Checksum"][0]["Value"]
    sqs_message = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "id": "test-id",
                        "filename": "test-filename",
                        "download_url": download_url,
                        "checksum": "some-incorrect-checksum",
                    }
                )
            }
        ]
    }
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        stream=True,
        status=200,
    )
    responses.add(
        responses.GET,
        checksum_url,
        json=example_checksum_response,
        status=200,
    )
    db_session.add(
        Granule(
            id="test-id",
            filename="test-filename",
            tileid="NM901",
            size=100,
            beginposition=datetime.now(),
            endposition=datetime.now(),
            ingestiondate=datetime.now(),
            download_url=download_url,
            downloaded=False,
            download_retries=1,
            checksum="some-incorrect-checksum",
        )
    )
    db_session.commit()

    handler(sqs_message, None)

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to(checksum_value)

    bucket_objects = list(mock_s3_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to("test-filename.zip")
    bucket_object_content = bucket_objects[0].get()["Body"].read().decode("utf-8")
    assert_that(bucket_object_content).contains("THIS IS A FAKE SAFE FILE")

    status = (
        db_session.query(Status)
        .filter(Status.key_name == "last_file_downloaded_time")
        .first()
    )
    assert_that(status.value).is_equal_to(str(datetime.now()))

    patched_logger.assert_has_calls(
        [
            mock.call("Received event to download image: test-filename"),
            mock.call("Successfully downloaded image: test-filename"),
        ]
    )
    responses.assert_call_count(checksum_url.replace(" ", "%20"), 1)
