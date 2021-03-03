import base64
import json
import logging
import os
from datetime import datetime
from typing import Tuple

import boto3
import requests
from botocore import client
from db.models.granule import Granule
from db.models.status import Status
from db.session import get_session, get_session_maker
from mypy_boto3_s3.client import S3Client
from sqlalchemy.exc import SQLAlchemyError

from exceptions import (
    ChecksumRetrievalException,
    FailedToDownloadFileException,
    FailedToUpdateGranuleDownloadFinishException,
    FailedToUpdateGranuleDownloadStartException,
    FailedToUploadFileException,
    GranuleAlreadyDownloadedException,
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

    try:
        granule = get_granule_and_set_download_started(image_id)
    except GranuleNotFoundException:
        return
    except GranuleAlreadyDownloadedException:
        return

    try:
        image_checksum = get_image_checksum(image_id)

        download_file(
            image_checksum,
            image_id,
            image_filename,
            download_url,
            granule.beginposition,
        )
    except Exception as ex:
        increase_retry_count(image_id)
        raise ex

    update_last_file_downloaded_time()


def get_granule_and_set_download_started(image_id: str) -> Granule:
    """
    Takes an `image_id` and returns the corresponding Granule from the `granule`
    database, the `download_started` value is populated to `datetime.now()` before it
    is returned.

    Checks are performed also to determine whether the granules retry limit is reached
    and whether it's already downloaded before we set download started
    :param image_id: str representing the id of the image in the `granule` table
    :returns: Granule representing the row in the `granule` table
    """
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        try:
            granule = db.query(Granule).filter(Granule.id == image_id).first()

            if granule:
                if granule.downloaded:
                    LOGGER.info(
                        f"Granule with id: {image_id} has already been downloaded"
                    )
                    raise GranuleAlreadyDownloadedException()
                elif granule.download_retries > 10:
                    error_message = (
                        f"Granule with id: {image_id} has reached its retry limit"
                    )
                    LOGGER.error(error_message)
                    raise RetryLimitReachedException(error_message)
                else:
                    granule.download_started = datetime.now()
                    db.commit()
                    db.refresh(granule)
                    return granule
            else:
                error_message = f"Granule with id: {image_id} not found"
                LOGGER.error(error_message)
                raise GranuleNotFoundException(error_message)
        except SQLAlchemyError as ex:
            db.rollback()
            error_message = (
                "Failed to update download_start value for granule with id: "
                f"{image_id}, exception was: {ex}"
            )
            LOGGER.error(error_message)
            raise FailedToUpdateGranuleDownloadStartException(error_message)


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
        error_message = (
            "There was an error retrieving the Checksum for Granule with id:"
            f" {image_id}. {str(ex)}"
        )
        LOGGER.error(error_message)
        raise ChecksumRetrievalException(error_message)


def download_file(
    image_checksum: str,
    image_id: str,
    image_filename: str,
    download_url: str,
    begin_position: datetime,
):
    """
    For a given image of id `image_id` and download location of `download_url`, make
    a request for the file and upload it to S3, if the checksum verification fails in s3
    the file will not be uploaded
    :param efs_mount_dir: str representing the path to the directory which EFS storage
        is mounted to
    :param image_checksum: str representing the Hex MD5 checksum that SciHub provides
    :param image_id: str representing the id of the image in the `granule` table
    :param image_filename: str representing the filename of the the image in the
        `granule` table
    :param download_url: str representing the SciHub URL to request the images file
        from
    :param begin_position: datetime representing the begin_position of the image in the
        `granule` table
    """
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        try:
            auth = get_scihub_auth()
            response = requests.get(url=download_url, auth=auth, stream=True)
            response.raise_for_status()

            aws_checksum = generate_aws_checksum(image_checksum)

            begin_position_str = begin_position.strftime("%Y-%m-%d")

            s3_client = get_s3_client()
            upload_bucket = os.environ["UPLOAD_BUCKET"]
            s3_client.put_object(
                Body=response.raw.read(),
                Bucket=upload_bucket,
                Key=f"{begin_position_str}/{image_filename}",
                ContentMD5=aws_checksum,
            )

            granule = db.query(Granule).filter(Granule.id == image_id).first()
            granule.downloaded = True
            granule.checksum = image_checksum
            granule.download_finished = datetime.now()
            db.commit()
        except requests.RequestException as ex:
            error_message = (
                "Requests exception thrown downloading granule with download_url:"
                f" {download_url}, exception was: {ex}"
            )
            LOGGER.error(error_message)
            raise FailedToDownloadFileException(error_message)
        except client.ClientError as ex:
            error_message = (
                f"Boto3 Client Error raised when uploading file: {image_filename}"
                f" for granule with id: {image_id}, error was: {ex}"
            )
            LOGGER.error(error_message)
            raise FailedToUploadFileException(error_message)
        except SQLAlchemyError as ex:
            db.rollback()
            error_message = (
                "SQLAlchemy Exception raised when updating download finish for"
                f" granule with id: {image_id}, exception was: {ex}"
            )
            LOGGER.error(error_message)
            raise FailedToUpdateGranuleDownloadFinishException(error_message)


def get_s3_client() -> S3Client:
    """
    Creates and returns a Boto3 Client for S3
    """
    return boto3.client("s3")


def generate_aws_checksum(image_checksum: str) -> str:
    """
    Takes an MD5 checksum provided by SciHub and generates a base64-encoded 128-bit
    version, which S3 will use to validate our file upload
    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object # Noqa
    for more information
    :param image_checksum: str representing the Hex MD5 checksum that SciHub provides
    :returns: str representing the base64-encoded checksum
    """
    return base64.b64encode(bytearray.fromhex(image_checksum)).decode("utf-8")


def increase_retry_count(image_id: str):
    """
    Takes a given granules id and increases its `download_retries` count by 1
    :param image_id: str representing the id of the image in the `granule` table
    """
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        granule = db.query(Granule).filter(Granule.id == image_id).first()
        granule.download_retries += 1
        db.commit()


def update_last_file_downloaded_time():
    """
    Updates the key with name `last_file_downloaded_time` in the `status` table
    to the current datetime
    """
    try:
        session_maker = get_session_maker()
        with get_session(session_maker) as db:
            status = (
                db.query(Status)
                .filter(Status.key_name == "last_file_downloaded_time")
                .first()
            )
            if status:
                status.value = datetime.now()
                db.commit()
            else:
                db.add(
                    Status(key_name="last_file_downloaded_time", value=datetime.now())
                )
                db.commit()
    except Exception as ex:
        LOGGER.error(
            (
                "Failed to update Status with key_name: last_file_downloaded_time, "
                f"exception was: {ex}"
            )
        )
