import json
from datetime import datetime
from unittest import mock

import pytest
import responses
from assertpy import assert_that
from botocore.client import ClientError
from db.models.granule import Granule
from freezegun import freeze_time

from exceptions import (
    ChecksumRetrievalException,
    FailedToDownloadFileException,
    FailedToUpdateGranuleDownloadFinishException,
    FailedToUpdateGranuleDownloadStartException,
    FailedToUploadFileException,
    GranuleNotFoundException,
    RetryLimitReachedException,
    SciHubAuthenticationNotRetrievedException,
)
from handler import (
    download_file,
    generate_aws_checksum,
    get_granule_and_set_download_started,
    get_image_checksum,
    get_scihub_auth,
    handler,
)


@freeze_time("2020-01-01 01:00:00")
def test_that_get_granule_returns_correct_granule(
    db_session,
):
    time_now = datetime.now()
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

    granule = get_granule_and_set_download_started("test-id")
    assert_that(granule.tileid).is_equal_to("NM901")
    assert_that(granule.size).is_equal_to(100)
    assert_that(granule.downloaded).is_false()
    assert_that(granule.download_started).is_equal_to(time_now)


def test_that_get_granule_throws_exception_when_no_granule_found():
    with pytest.raises(GranuleNotFoundException) as ex:
        get_granule_and_set_download_started("test-id")
    assert_that(str(ex.value)).is_equal_to("Granule with id: test-id not found")


@freeze_time("2020-01-01 01:00:00")
def test_that_get_granule_rollsback_and_throws_error_if_error_updating_download_started(
    db_session, fake_db_session_that_fails
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
            download_started=datetime(2020, 1, 1, 0, 0, 0),
        )
    )
    db_session.commit()

    with mock.patch("handler.get_session", fake_db_session_that_fails):
        with pytest.raises(FailedToUpdateGranuleDownloadStartException) as ex:
            get_granule_and_set_download_started("test-id")
        assert_that(str(ex.value)).is_equal_to(
            (
                "Failed to update download_start value for granule with id: test-id,"
                " exception was: An Exception"
            )
        )

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.download_started).is_equal_to(datetime(2020, 1, 1, 0, 0, 0))


def test_that_scihub_credentials_loaded_correctly(
    mock_scihub_credentials,
):
    auth = get_scihub_auth()
    assert_that(auth[0]).is_equal_to(mock_scihub_credentials["username"])
    assert_that(auth[1]).is_equal_to(mock_scihub_credentials["password"])


def test_that_exception_thrown_if_error_in_retrieving_scihub_credentials():
    with mock.patch("handler.boto3.client") as patch_boto:
        patch_boto.side_effect = Exception("An exception")
        with pytest.raises(SciHubAuthenticationNotRetrievedException) as ex:
            get_scihub_auth()
        assert_that(str(ex.value)).is_equal_to(
            "There was an error retrieving SciHub Credentials: An exception"
        )


@responses.activate
def test_that_get_image_checksum_returns_correct_value(example_checksum_response):
    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/"
            "?$format=json&$select=Checksum"
        ),
        json=example_checksum_response,
        status=200,
    )
    expected_checksum_value = example_checksum_response["d"]["Checksum"]["Value"]
    checksum_value = get_image_checksum("test-id")
    assert_that(checksum_value).is_equal_to(expected_checksum_value)


@responses.activate
def test_exception_thrown_if_error_in_retrieving_image_checksum():
    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/"
            "?$format=json&$select=Checksum"
        ),
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
    db_session, mock_scihub_credentials
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=404,
    )

    with pytest.raises(FailedToDownloadFileException) as ex:
        download_file(
            "ACHECKSUM", "test-id", "test-filename.SAFE", download_url, datetime.now()
        )
    assert_that(str(ex.value)).is_equal_to(
        (
            "Requests exception thrown downloading granule with "
            f"download_url: {download_url}, exception was: 404 Client Error: "
            "Not Found"
            " for url: https://scihub.copernicus.eu/dhus/odata/v1/"
            "Products('test-id')"
            "/$value"
        )
    )


@responses.activate
def test_that_download_file_correctly_raises_exception_if_s3_upload_fails(
    db_session, mock_s3_bucket, mock_scihub_credentials
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
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
                datetime.now(),
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
    db_session, mock_s3_bucket, mock_scihub_credentials, fake_db_session_that_fails
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
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
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
                datetime.now(),
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
    mock_scihub_credentials,
    mock_s3_bucket,
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
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        status=200,
        stream=True,
    )
    patched_generate_aws_checksum.return_value = "an-aws-checksum"

    download_file(
        "ACHECKSUM", "test-id", "test-filename.SAFE", download_url, datetime.now()
    )

    patched_generate_aws_checksum.assert_called_once_with("ACHECKSUM")

    bucket_objects = list(mock_s3_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to("2020-01-01/test-filename.SAFE")
    bucket_object_content = bucket_objects[0].get()["Body"].read().decode("utf-8")
    assert_that(bucket_object_content).contains("THIS IS A FAKE SAFE FILE")

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to("ACHECKSUM")
    assert_that(granule.download_finished).is_equal_to(datetime.now())


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


def test_that_handler_correctly_logs_and_returns_if_error_updating_granule_download_start(  # Noqa
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
        "Failed to update download_start value for granule with id: test-id, exception"
        " was: An Exception"
    )

    with mock.patch("handler.get_session", fake_db_session_that_fails):
        with mock.patch("handler.LOGGER.error") as patched_logger:
            with pytest.raises(FailedToUpdateGranuleDownloadStartException) as ex:
                handler(sqs_message, None)
            patched_logger.assert_called_once_with(expected_error_message)
            assert_that(str(ex.value)).is_equal_to(expected_error_message)


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
        patched_logger.assert_called_once_with(
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

    with mock.patch("handler.LOGGER.error") as patched_logger:
        with pytest.raises(RetryLimitReachedException):
            handler(sqs_message, None)
        patched_logger.assert_called_once_with(
            "Granule with id: test-id has reached its retry limit"
        )


@responses.activate
def test_that_handler_correctly_logs_and_errors_if_get_image_checksum_fails(db_session):
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
        with mock.patch("handler.get_scihub_auth") as patched_auth:
            patched_auth.side_effect = Exception("An exception")
            with pytest.raises(ChecksumRetrievalException):
                handler(sqs_message, None)
            patched_logger.assert_called_once_with(
                (
                    "There was an error retrieving the Checksum for Granule with id:"
                    " test-id. An exception"
                )
            )


@responses.activate
@mock.patch("handler.get_image_checksum")
def test_that_handler_correctly_logs_and_errors_if_image_fails_to_download(
    mock_get_image_checksum, db_session
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
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
                        "download_url": (
                            "https://scihub.copernicus.eu/dhus/odata/v1/"
                            "Products('test-id')/$value"
                        ),
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

    expected_error_message = (
        "Requests exception thrown downloading granule with download_url:"
        f" {download_url}, exception was: 404 Client Error: Not Found for url: "
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )

    mock_get_image_checksum.return_value = "test-checksum"
    with mock.patch("handler.LOGGER.error") as mock_logger:
        with pytest.raises(FailedToDownloadFileException) as ex:
            handler(sqs_message, None)
        mock_logger.assert_called_once_with(expected_error_message)
    assert_that(str(ex.value)).is_equal_to(expected_error_message)


@responses.activate
@mock.patch("handler.get_image_checksum")
def test_that_handler_correctly_logs_and_errors_if_image_fails_to_upload(
    mock_get_image_checksum, db_session, mock_s3_bucket
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
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
                        "download_url": (
                            "https://scihub.copernicus.eu/dhus/odata/v1/"
                            "Products('test-id')/$value"
                        ),
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

    expected_error_message = (
        "Boto3 Client Error raised when uploading file: test-filename.SAFE for granule"
        " with id: test-id, error was: An error occurred (500) when calling the "
        "Something Broke operation: Something Broke"
    )

    mock_get_image_checksum.return_value = "36F3AB53F6D2D9592CF50CE4682FF7EA"
    with mock.patch("handler.get_s3_client", FakeClient):
        with mock.patch("handler.LOGGER.error") as mock_logger:
            with pytest.raises(FailedToUploadFileException) as ex:
                handler(sqs_message, None)
            mock_logger.assert_called_once_with(expected_error_message)
        assert_that(str(ex.value)).is_equal_to(expected_error_message)


@responses.activate
@mock.patch("handler.get_image_checksum")
@mock.patch("handler.download_file")
def test_that_handler_correctly_logs_and_errors_if_update_download_finish_fails(
    mock_download_file,
    mock_get_image_checksum,
    db_session,
    mock_s3_bucket,
    fake_db_session_that_fails,
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
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
                        "download_url": (
                            "https://scihub.copernicus.eu/dhus/odata/v1/"
                            "Products('test-id')/$value"
                        ),
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

    mock_download_file.side_effect = FailedToUpdateGranuleDownloadFinishException(
        "An Exception"
    )

    mock_get_image_checksum.return_value = "36F3AB53F6D2D9592CF50CE4682FF7EA"
    with mock.patch("handler.LOGGER.error") as mock_logger:
        with pytest.raises(FailedToUpdateGranuleDownloadFinishException) as ex:
            handler(sqs_message, None)
        mock_logger.assert_called_once_with("An Exception")
    assert_that(str(ex.value)).is_equal_to("An Exception")


@responses.activate
@freeze_time("2020-02-02 00:00:00")
def test_that_handler_correctly_downloads_file_and_updates_granule(
    db_session,
    fake_safe_file_contents,
    mock_s3_bucket,
    mock_scihub_credentials,
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
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
    responses.add(
        responses.GET,
        (
            "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/"
            "?$format=json&$select=Checksum"
        ),
        json={"d": {"Checksum": {"Value": "36F3AB53F6D2D9592CF50CE4682FF7EA"}}},
        status=200,
    )
    db_session.add(
        Granule(
            id="test-id",
            filename="a-filename",
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
    assert_that(granule.download_started).is_equal_to(datetime.now())
    assert_that(granule.download_finished).is_equal_to(datetime.now())
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to("36F3AB53F6D2D9592CF50CE4682FF7EA")

    bucket_objects = list(mock_s3_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to("2020-02-02/test-filename")
    bucket_object_content = bucket_objects[0].get()["Body"].read().decode("utf-8")
    assert_that(bucket_object_content).contains("THIS IS A FAKE SAFE FILE")
