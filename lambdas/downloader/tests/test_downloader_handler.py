import json
import os
from datetime import datetime
from unittest import mock

import pytest
import responses
from assertpy import assert_that
from db.models.granule import Granule
from freezegun import freeze_time

from exceptions import (
    ChecksumRetrievalException,
    FailedToDownloadFileException,
    FailedToHandleInvalidFileException,
    FailedToHandleValidFileException,
    FailedToUpdateGranuleDownloadStartException,
    GranuleNotFoundException,
    RetryLimitReachedException,
    SciHubAuthenticationNotRetrievedException,
)
from handler import (
    download_file,
    file_already_exists,
    get_file_checksum,
    get_granule_and_set_download_started,
    get_image_checksum,
    get_scihub_auth,
    handle_file,
    handle_invalid_file,
    handle_valid_file,
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
                " exception was: An exception"
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


def test_that_file_already_exists_returns_correctly_when_file_does_exist(tmpdir):
    tmpdir.mkdir("test-id").join("test-filename").write(b"")
    file_exists = file_already_exists(tmpdir.realpath(), "test-id", "test-filename")
    assert_that(file_exists).is_true()


def test_that_file_already_exists_returns_correctly_when_file_does_not_exist(tmpdir):
    file_exists = file_already_exists(tmpdir.dirpath(), "test-id", "test-filename")
    assert_that(file_exists).is_false()


def test_that_correct_checksum_is_returned_for_file(tmpdir, checksum_file_contents):
    expected_checksum = "A50E6DC6C2B8F57EFD9AE279461122C6"
    tmpdir.mkdir("test-id").join("test-filename").write(checksum_file_contents)
    actual_checksum = get_file_checksum(tmpdir.realpath(), "test-id", "test-filename")
    assert_that(expected_checksum).is_equal_to(actual_checksum)


def test_that_handle_file_for_matching_checksums_invokes_correct_function():
    with mock.patch("handler.get_file_checksum") as mock_file_checksum:
        mock_file_checksum.return_value = "test-checksum"
        with mock.patch("handler.handle_valid_file") as mock_handle_valid_file:
            handle_file("test-dir", "test-checksum", "test-id", "test-filename", {})
            mock_handle_valid_file.assert_called_once_with(
                "test-checksum", "test-id", "test-filename"
            )


def test_that_handle_file_for_non_matching_checksums_invokes_correct_function():
    with mock.patch("handler.get_file_checksum") as mock_file_checksum:
        mock_file_checksum.return_value = "a-different-checksum"
        with mock.patch("handler.handle_invalid_file") as mock_handle_invalid_file:
            handle_file("test-dir", "test-checksum", "test-id", "test-filename", {})
            mock_handle_invalid_file.assert_called_once_with(
                "test-dir", "test-id", "test-filename", {}
            )


def test_that_handle_invalid_file_correctly_clears_up(
    tmpdir, db_session, mock_sqs_download_queue
):
    tmpdir.mkdir("test-id").join("test-filename").write(b"")
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

    image_message = {
        "id": "test-id",
        "filename": "test-filename",
        "download_url": "test-url",
    }
    handle_invalid_file(tmpdir.realpath(), "test-id", "test-filename", image_message)

    file_exists = os.path.exists(
        os.path.join(tmpdir.realpath(), "test-id", "test-filename")
    )
    assert_that(file_exists).is_false()

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.download_retries).is_equal_to(6)

    mock_sqs_download_queue.load()
    number_of_messages_in_queue = mock_sqs_download_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(1)
    message = mock_sqs_download_queue.receive_messages(MaxNumberOfMessages=1)[0]
    message_body = json.loads(message.body)
    assert_that(message_body).is_equal_to(image_message)


def test_that_handle_invalid_file_raises_exception_if_error_caught_clearing_up(
    db_session, tmpdir, fake_db_session_that_fails
):
    tmpdir.mkdir("test-id").join("test-filename").write(b"")
    image_message = {
        "id": "test-id",
        "filename": "test-filename",
        "download_url": "test-url",
    }
    with pytest.raises(FailedToHandleInvalidFileException) as ex:
        with mock.patch("handler.get_session", fake_db_session_that_fails):
            handle_invalid_file(
                tmpdir.realpath(), "test-id", "test-filename", image_message
            )
    assert_that(str(ex.value)).is_equal_to(
        (
            "Failed to cleanup after invalid file failure for granule with id: test-id,"
            " exception was: An exception"
        )
    )


@freeze_time("2020-01-01 00:00:00")
def test_that_handle_valid_file_updates_db_and_populates_queue_correctly(
    db_session, mock_sqs_upload_queue
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

    handle_valid_file("test-checksum", "test-id", "test-filename")

    granule = db_session.query(Granule).filter(Granule.id == "test-id").first()
    assert_that(granule.downloaded).is_true()
    assert_that(granule.checksum).is_equal_to("test-checksum")
    assert_that(granule.download_finished).is_equal_to(datetime.now())

    mock_sqs_upload_queue.load()
    number_of_messages_in_queue = mock_sqs_upload_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(1)
    message = mock_sqs_upload_queue.receive_messages(MaxNumberOfMessages=1)[0]
    message_body = json.loads(message.body)
    assert_that(message_body).is_equal_to(
        {"id": "test-id", "filename": "test-filename"}
    )


def test_that_handle_valid_file_raises_exception_if_error_caught(
    db_session, fake_db_session_that_fails
):
    with pytest.raises(FailedToHandleValidFileException) as ex:
        with mock.patch("handler.get_session", fake_db_session_that_fails):
            handle_valid_file("test-checksum", "test-id", "test-filename")
    assert_that(str(ex.value)).is_equal_to(
        (
            "Failed to handle a download for granule with id: test-id,"
            " exception was: An exception"
        )
    )


@responses.activate
def test_that_download_file_correctly_saves_file_if_request_successful_and_dir_present(
    fake_safe_file_contents, tmpdir, mock_scihub_credentials
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        status=200,
    )

    tmpdir.mkdir("test-id")
    download_file(tmpdir.realpath(), "test-id", "test-filename.SAFE", download_url)

    expected_path = os.path.join(tmpdir.realpath(), "test-id", "test-filename.SAFE")
    file_exists = os.path.exists(expected_path)
    assert_that(file_exists).is_true()

    with open(expected_path, "r") as expected_in:
        contents = expected_in.read().replace("\n", "")
    assert_that(contents).is_equal_to("THIS IS A FAKE SAFE FILE")


@responses.activate
def test_that_download_file_correctly_saves_file_if_request_successful_and_dir_not_present(  # Noqa
    fake_safe_file_contents, tmpdir, mock_scihub_credentials
):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=fake_safe_file_contents,
        status=200,
    )
    download_file(tmpdir.realpath(), "test-id", "test-filename.SAFE", download_url)

    expected_path = os.path.join(tmpdir.realpath(), "test-id", "test-filename.SAFE")
    file_exists = os.path.exists(expected_path)
    assert_that(file_exists).is_true()

    with open(expected_path, "r") as expected_in:
        contents = expected_in.read().replace("\n", "")
    assert_that(contents).is_equal_to("THIS IS A FAKE SAFE FILE")


@responses.activate
def test_that_download_file_correctly_raises_exception_if_request_not_succesful(tmpdir):
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
        download_file(tmpdir.realpath(), "test-id", "test-filename.SAFE", download_url)
    assert_that(str(ex.value)).is_equal_to(
        (
            "Requests exception thrown downloading granule with "
            f"download_url: {download_url}, exception was: 404 Client Error: Not Found"
            " for url: https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')"
            "/$value"
        )
    )


@responses.activate
def test_that_download_file_correctly_raises_exception_if_io_error(tmpdir):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
    )

    with pytest.raises(FailedToDownloadFileException) as ex:
        download_file("non-existent", "test-id", "test-filename.SAFE", download_url)
    assert_that(str(ex.value)).is_equal_to(
        (
            "Exception thrown permorming IO when downloading granule with download_url:"
            f" {download_url}, exception was: [Errno 2] No such"
            " file or directory: 'non-existent/test-id'"
        )
    )


@responses.activate
def test_that_download_file_correctly_raises_exception_if_any_other_error(tmpdir):
    download_url = (
        "https://scihub.copernicus.eu/dhus/odata/v1/Products('test-id')/$value"
    )
    responses.add(
        responses.GET,
        download_url,
        body=b"",
        status=200,
    )

    with pytest.raises(FailedToDownloadFileException) as ex:
        with mock.patch("handler.os.path.join") as mock_join:
            mock_join.side_effect = Exception("A generic exception")
            download_file("non-existent", "test-id", "test-filename.SAFE", download_url)
    assert_that(str(ex.value)).is_equal_to(
        (
            f"Exception thrown downloading granule with download_url: {download_url},"
            " exception was: A generic exception"
        )
    )


def test_that_handler_correctly_logs_and_returns_if_no_granule_found(fake_efs_mount):
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


def test_that_handler_correctly_logs_and_returns_if_already_downloaded(
    db_session, fake_efs_mount
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
    db_session, fake_efs_mount
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
def test_that_handler_correctly_logs_and_errors_if_get_image_checksum_fails(
    db_session, fake_efs_mount
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


@mock.patch("handler.handle_file")
@mock.patch("handler.file_already_exists")
@mock.patch("handler.get_image_checksum")
def test_that_handler_correctly_logs_and_errors_if_failed_to_handle_valid_file(
    mock_get_image_checksum,
    mock_file_already_exists,
    mock_handle_file,
    db_session,
    fake_efs_mount,
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

    # File exists
    mock_get_image_checksum.return_value = "test-checksum"
    mock_file_already_exists.return_value = True
    mock_handle_file.side_effect = FailedToHandleValidFileException("An exception")
    with mock.patch("handler.LOGGER.error") as mock_logger:
        with pytest.raises(FailedToHandleValidFileException) as ex:
            handler(sqs_message, None)
        mock_logger.assert_called_once_with("An exception")
    assert_that(str(ex.value)).is_equal_to("An exception")

    # File doesn't exist
    mock_get_image_checksum.return_value = "test-checksum"
    mock_file_already_exists.return_value = False
    mock_handle_file.side_effect = FailedToHandleValidFileException("An exception")
    with mock.patch("handler.download_file") as mock_download_file:
        mock_download_file.return_value = None
        with mock.patch("handler.LOGGER.error") as mock_logger:
            with pytest.raises(FailedToHandleValidFileException) as ex:
                handler(sqs_message, None)
            mock_logger.assert_called_once_with("An exception")
        assert_that(str(ex.value)).is_equal_to("An exception")


@mock.patch("handler.handle_file")
@mock.patch("handler.file_already_exists")
@mock.patch("handler.get_image_checksum")
def test_that_handler_correctly_logs_and_errors_if_failed_to_handle_invalid_file(
    mock_get_image_checksum,
    mock_file_already_exists,
    mock_handle_file,
    db_session,
    fake_efs_mount,
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

    # File exists
    mock_get_image_checksum.return_value = "test-checksum"
    mock_file_already_exists.return_value = True
    mock_handle_file.side_effect = FailedToHandleInvalidFileException("An exception")
    with mock.patch("handler.LOGGER.error") as mock_logger:
        with pytest.raises(FailedToHandleInvalidFileException) as ex:
            handler(sqs_message, None)
        mock_logger.assert_called_once_with("An exception")
    assert_that(str(ex.value)).is_equal_to("An exception")

    # File doesn't exist
    mock_get_image_checksum.return_value = "test-checksum"
    mock_file_already_exists.return_value = False
    mock_handle_file.side_effect = FailedToHandleInvalidFileException("An exception")
    with mock.patch("handler.download_file") as mock_download_file:
        mock_download_file.return_value = None
        with mock.patch("handler.LOGGER.error") as mock_logger:
            with pytest.raises(FailedToHandleInvalidFileException) as ex:
                handler(sqs_message, None)
            mock_logger.assert_called_once_with("An exception")
        assert_that(str(ex.value)).is_equal_to("An exception")


@mock.patch("handler.file_already_exists")
@mock.patch("handler.get_image_checksum")
def test_that_handler_correctly_logs_and_errors_if_failed_to_download_file(
    mock_get_image_checksum, mock_file_already_exists, db_session, fake_efs_mount
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

    mock_get_image_checksum.return_value = "test-checksum"
    mock_file_already_exists.return_value = False
    with mock.patch("handler.download_file") as mock_download_file:
        mock_download_file.side_effect = FailedToDownloadFileException("An exception")
        with mock.patch("handler.LOGGER.error") as mock_logger:
            with pytest.raises(FailedToDownloadFileException) as ex:
                handler(sqs_message, None)
            mock_logger.assert_called_once_with("An exception")
        assert_that(str(ex.value)).is_equal_to("An exception")


def test_that_handler_correctly_logs_and_errors_if_failed_get_and_update_granule(
    db_session, fake_efs_mount, fake_db_session_that_fails
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

    with mock.patch("handler.LOGGER.error") as mock_logger:
        with mock.patch("handler.get_session", fake_db_session_that_fails):
            with pytest.raises(FailedToUpdateGranuleDownloadStartException):
                handler(sqs_message, None)
            mock_logger.assert_called_once_with(
                (
                    "Failed to update download_start value for granule with id: test-id"
                    ", exception was: An exception"
                )
            )


@mock.patch("handler.handle_file")
@mock.patch("handler.get_image_checksum")
def test_that_handler_downloads_file_if_missing(
    mock_get_image_checksum, mock_handle_file, db_session, fake_efs_mount
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

    mock_get_image_checksum.return_value = "test-checksum"
    mock_handle_file.return_value = None
    with mock.patch("handler.download_file") as mock_download_file:
        handler(sqs_message, None)
        mock_download_file.assert_called_once_with(
            str(fake_efs_mount.realpath()), "test-id", "test-filename", "test-url"
        )


@mock.patch("handler.handle_file")
@mock.patch("handler.get_image_checksum")
def test_that_handler_doesnt_download_file_if_present(
    mock_get_image_checksum, mock_handle_file, db_session, fake_efs_mount
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
    fake_efs_mount.mkdir("test-id").join("test-filename").write(b"")
    mock_get_image_checksum.return_value = "test-checksum"
    mock_handle_file.return_value = None
    with mock.patch("handler.download_file") as mock_download_file:
        handler(sqs_message, None)
        mock_download_file.assert_not_called()


@responses.activate
@freeze_time("2020-02-02 00:00:00")
def test_that_handler_correctly_downloads_file_and_updates_granule(
    db_session,
    fake_efs_mount,
    fake_safe_file_contents,
    mock_sqs_upload_queue,
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

    mock_sqs_upload_queue.load()
    number_of_messages_in_queue = mock_sqs_upload_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(1)
    message = mock_sqs_upload_queue.receive_messages(MaxNumberOfMessages=1)[0]
    message_body = json.loads(message.body)
    assert_that(message_body).is_equal_to(
        {"id": "test-id", "filename": "test-filename"}
    )

    file_exists = os.path.exists(
        os.path.join(fake_efs_mount.realpath(), "test-id", "test-filename")
    )
    assert_that(file_exists).is_true()
