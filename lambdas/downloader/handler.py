import json
import logging
import os
from datetime import datetime
from hashlib import md5
from typing import Tuple

import boto3
import requests
from db.models.granule import Granule
from db.session import get_session, get_session_maker

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

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SCIHUB_URL = os.environ.get("SCIHUB_URL", "https://scihub.copernicus.eu")
SCIHUB_CHECKSUM_URL_FMT = (
    f"{SCIHUB_URL}/dhus/odata/v1/Products('{{}}')/?$format=json&$select=Checksum"
)


def handler(event, context):
    image_message = json.loads(event["Records"][0]["body"])
    image_id = image_message["id"]
    image_filename = image_message["filename"]
    download_url = image_message["download_url"]
    efs_mount_dir = os.environ["EFS_MOUNT_DIR"]

    try:
        granule = get_granule_and_set_download_started(image_id)
    except GranuleNotFoundException as ex:
        LOGGER.error(str(ex))
        return
    except FailedToUpdateGranuleDownloadStartException as ex:
        LOGGER.error(str(ex))
        raise ex

    if granule.downloaded:
        LOGGER.info(f"Granule with id: {image_id} has already been downloaded")
        return

    if granule.download_retries > 10:
        error_message = f"Granule with id: {image_id} has reached its retry limit"
        LOGGER.error(error_message)
        raise RetryLimitReachedException(error_message)

    try:
        image_checksum = get_image_checksum(image_id)
    except ChecksumRetrievalException as ex:
        LOGGER.error(str(ex))
        raise ex

    try:
        if file_already_exists(efs_mount_dir, image_id, image_filename):
            handle_file(
                efs_mount_dir, image_checksum, image_id, image_filename, image_message
            )
            return
        else:
            download_file(efs_mount_dir, image_id, image_filename, download_url)
            handle_file(
                efs_mount_dir, image_checksum, image_id, image_filename, image_message
            )
            return
    except FailedToHandleValidFileException as ex:
        LOGGER.error(str(ex))
        raise ex
    except FailedToHandleInvalidFileException as ex:
        LOGGER.error(str(ex))
        raise ex
    except FailedToDownloadFileException as ex:
        LOGGER.error(str(ex))
        raise ex


def get_granule_and_set_download_started(image_id: str) -> Granule:
    """
    Takes an `image_id` and returns the corresponding Granule from the `granule`
    database, the `download_started` value is populated to `datetime.now()` before it
    is returned
    """
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        try:
            granule = db.query(Granule).filter(Granule.id == image_id).first()
            if granule:
                granule.download_started = datetime.now()
                db.commit()
        except Exception as ex:
            db.rollback()
            raise FailedToUpdateGranuleDownloadStartException(
                (
                    "Failed to update download_start value for granule with id: "
                    f"{image_id}, exception was: {ex}"
                )
            )
    if granule:
        return granule
    else:
        raise GranuleNotFoundException(f"Granule with id: {image_id} not found")


def get_scihub_auth() -> Tuple[str, str]:
    """
    Retrieves the username and password for SciHub which is stored in SecretsManager
    :returns: Tuple[str, str] representing the SciHub username and password
    """
    try:
        stage = os.environ["STAGE"]
        secrets_manager_client = boto3.client("secretsmanager")
        secret = json.loads(
            secrets_manager_client.get_secret_value(
                SecretId=f"hls-s2-downloader-serverless/{stage}/scihub-credentials"
            )["SecretString"]
        )
        return (secret["username"], secret["password"])
    except Exception as ex:
        raise SciHubAuthenticationNotRetrievedException(
            f"There was an error retrieving SciHub Credentials: {str(ex)}"
        )


def get_image_checksum(image_id: str) -> str:
    """
    Takes an `image_id` of a granule and retrieves its Checksum value from the
    SciHub API
    :param image_id: str representing the id of the image in the `granule` table
    :returns: str representing the Checksum value returned from the SciHub API
    """
    try:
        auth = get_scihub_auth()
        response = requests.get(url=SCIHUB_CHECKSUM_URL_FMT.format(image_id), auth=auth)
        response.raise_for_status()

        return response.json()["d"]["Checksum"]["Value"]
    except Exception as ex:
        raise ChecksumRetrievalException(
            (
                "There was an error retrieving the Checksum"
                f" for Granule with id: {image_id}. {str(ex)}"
            )
        )


def file_already_exists(efs_mount_dir: str, image_id: str, image_filename: str) -> bool:
    """
    Takes a `image_id` and a `image_filename` and determines whether the file exists
    on the mounted disc or not
    :param efs_mount_dir: str representing the path to the directory which EFS storage
        is mounted to
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    :returns: bool for whether the file exists or not
    """
    return os.path.exists(os.path.join(efs_mount_dir, image_id, image_filename))


def get_file_checksum(efs_mount_dir: str, image_id: str, image_filename: str) -> str:
    """
    Takes a `image_id` and a `image_filename` and returns the MD5 checksum of the file
    on the mounted disc
    :param efs_mount_dir: str representing the path to the directory which EFS storage
        is mounted to
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    :returns: str representing the MD5 checksum of the file
    """
    with open(os.path.join(efs_mount_dir, image_id, image_filename), "rb") as file_in:
        return md5(file_in.read()).hexdigest().upper()


def handle_file(
    efs_mount_dir: str,
    image_checksum: str,
    image_id: str,
    image_filename: str,
    image_message: dict,
):
    """
    For a given granule with id `image_id`, determine whether its checksum matches
    its local checksum and handle it accordingly
    :param efs_mount_dir: str representing the path to the directory which EFS storage
        is mounted to
    :param image_checksum: str representing the checksum of the image with id `image_id`
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    :param image_message: dict representing the original message from SQS to be
        re-delivered
    """
    if image_checksum == get_file_checksum(efs_mount_dir, image_id, image_filename):
        handle_valid_file(image_checksum, image_id, image_filename)
    else:
        handle_invalid_file(efs_mount_dir, image_id, image_filename, image_message)


def handle_invalid_file(
    efs_mount_dir: str, image_id: str, image_filename: str, image_message: dict
):
    """
    For a given failed download or checksum match, this deletes the file from the EFS
    mount, increments the granule with id `image_id` `download_retries` value by 1,
    re-adds the invoking message back onto the `TO_DOWNLOAD` queue and raises an
    error to be caught
    :param efs_mount_dir: str representing the path to the directory which EFS storage
        is mounted to
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    :param image_message: dict representing the original message from SQS to be
        re-delivered
    """
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        try:
            os.remove(os.path.join(efs_mount_dir, image_id, image_filename))

            granule = db.query(Granule).filter(Granule.id == image_id).first()
            granule.download_retries += 1
            db.commit()

            sqs_client = boto3.client("sqs")
            to_download_queue_url = os.environ["TO_DOWNLOAD_SQS_QUEUE_URL"]
            sqs_client.send_message(
                QueueUrl=to_download_queue_url, MessageBody=json.dumps(image_message)
            )
        except Exception as ex:
            db.rollback()
            raise FailedToHandleInvalidFileException(
                (
                    "Failed to cleanup after invalid file failure for "
                    f"granule with id: {image_id}, exception was: {ex}"
                )
            )


def handle_valid_file(image_checksum: str, image_id: str, image_filename: str):
    """
    For a given valid checksum match, this updates the granule with id `image_id`
    `downloaded` value to be True, its checksum value, and adds a message to the
    `TO_UPLOAD_QUEUE`
    :param image_checksum: str representing the checksum of the image with id `image_id`
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    """
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        try:
            granule = db.query(Granule).filter(Granule.id == image_id).first()
            granule.downloaded = True
            granule.checksum = image_checksum
            granule.download_finished = datetime.now()
            db.commit()

            sqs_client = boto3.client("sqs")
            to_upload_queue_url = os.environ["TO_UPLOAD_SQS_QUEUE_URL"]
            sqs_client.send_message(
                QueueUrl=to_upload_queue_url,
                MessageBody=json.dumps({"id": image_id, "filename": image_filename}),
            )
        except Exception as ex:
            db.rollback()
            raise FailedToHandleValidFileException(
                (
                    f"Failed to handle a download for granule with id: {image_id},"
                    f" exception was: {ex}"
                )
            )


def download_file(
    efs_mount_dir: str, image_id: str, image_filename: str, download_url: str
):
    """
    For a given image of id `image_id` and download location of `download_url`, make
    a request for the file and save it under `efs_mount_dir/image_id/image_filename`
    :param efs_mount_dir: str representing the path to the directory which EFS storage
        is mounted to
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    :param download_url: str representing the SciHub URL to request the images file
        from
    """
    try:
        auth = get_scihub_auth()
        response = requests.get(url=download_url, auth=auth)
        response.raise_for_status()

        image_base_dir = os.path.join(efs_mount_dir, image_id)
        if not os.path.exists(image_base_dir):
            os.mkdir(image_base_dir)

        with open(
            os.path.join(efs_mount_dir, image_id, image_filename), "wb"
        ) as file_out:
            file_out.write(response.content)

    except requests.RequestException as ex:
        raise FailedToDownloadFileException(
            (
                "Requests exception thrown downloading granule with download_url:"
                f" {download_url}, exception was: {ex}"
            )
        )
    except OSError as ex:
        raise FailedToDownloadFileException(
            (
                "Exception thrown permorming IO when downloading granule with "
                f"download_url: {download_url}, exception was: {ex}"
            )
        )
    except Exception as ex:
        raise FailedToDownloadFileException(
            (
                "Exception thrown downloading granule with download_url:"
                f" {download_url}, exception was: {ex}"
            )
        )
