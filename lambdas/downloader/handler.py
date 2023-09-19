import base64
import json
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, TypedDict

import boto3
import requests
from botocore import client
from db.models.granule import Granule
from db.models.status import Status
from db.session import get_session, get_session_maker
from exceptions import (
    ChecksumRetrievalException,
    CopernicusTokenNotRetrievedException,
    FailedToDownloadFileException,
    FailedToRetrieveGranuleException,
    FailedToUpdateGranuleDownloadFinishException,
    FailedToUploadFileException,
    GranuleAlreadyDownloadedException,
    GranuleNotFoundException,
    RetryLimitReachedException,
)
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

COPERNICUS_ZIPPER_URL = os.environ.get(
    "COPERNICUS_ZIPPER_URL",
    "http://zipper.dataspace.copernicus.eu",
)
COPERNICUS_CHECKSUM_URL = os.environ.get(
    "COPERNICUS_CHECKSUM_URL",
    "https://catalogue.dataspace.copernicus.eu",
)
COPERNICUS_IDENTITY_URL = os.environ.get(
    "COPERNICUS_IDENTITY_URL",
    "https://identity.dataspace.copernicus.eu/auth/realms/"
    "CDSE/protocol/openid-connect/token",
)


class CopernicusCredentials(TypedDict):
    username: str
    password: str


def handler(event, context):
    image_message = json.loads(event["Records"][0]["body"])
    image_id = image_message["id"]
    image_filename = image_message["filename"]
    download_url = get_download_url(image_message["id"])

    LOGGER.info(f"Received event to download image: {image_filename}")

    try:
        get_granule(image_id)
    except GranuleNotFoundException as e:
        LOGGER.error(str(e))
        return
    except GranuleAlreadyDownloadedException as e:
        LOGGER.info(str(e))
        return

    try:
        image_checksum = get_image_checksum(image_id)

        download_file(
            image_checksum,
            image_id,
            image_filename,
            download_url,
        )

        LOGGER.info(f"Successfully downloaded image: {image_filename}")
    except Exception:
        increase_retry_count(image_id)
        raise

    update_last_file_downloaded_time()


def get_download_url(image_id: str) -> str:
    """
    Takes the `image_id` value from `image_message` and returns a
    the zipper download url.
    """
    return f"{COPERNICUS_ZIPPER_URL}/odata/v1/Products({image_id})/$value"


def get_granule(image_id: str) -> Granule:
    """
    Takes an `image_id` and returns the corresponding Granule from the `granule`
    database.

    Checks are performed also to determine whether the granules retry limit is reached
    and whether it's already downloaded before return it
    :param image_id: str representing the id of the image in the `granule` table
    :returns: Granule representing the row in the `granule` table
    """
    session_maker = get_session_maker()

    with get_session(session_maker) as db:
        try:
            if not (
                granule := db.query(Granule).filter(Granule.id == image_id).first()
            ):
                raise GranuleNotFoundException(f"Granule with id: {image_id} not found")
            if granule.downloaded:
                raise GranuleAlreadyDownloadedException(
                    f"Granule with id: {image_id} has already been downloaded"
                )
            if granule.download_retries > 10:
                raise RetryLimitReachedException(
                    f"Granule with id: {image_id} has reached its retry limit"
                )

            db.refresh(granule)

            return granule
        except SQLAlchemyError as ex:
            db.rollback()

            raise FailedToRetrieveGranuleException(
                f"Failed to retrieve granule with id: {image_id}, exception was: {ex}"
            ) from None


def get_copernicus_token() -> str:
    """
    Retrieves keycloak token from parameter store.
    :returns: str of token
    """
    try:
        stage = os.environ["STAGE"]
        ssm_client = boto3.client("ssm")
        token_parameter = ssm_client.get_parameter(
            Name=f"/hls-s2-downloader-serverless/{stage}/copernicus-token",
        )
        return token_parameter["Parameter"]["Value"]
    except Exception as ex:
        raise CopernicusTokenNotRetrievedException(
            f"There was error retrieving the keycloak token {ex}"
        ) from None


def get_image_checksum(image_id: str) -> str:
    """
    Takes an `image_id` of a granule and retrieves its Checksum value from the
    SciHub API
    :param image_id: str representing the id of the image in the `granule` table
    :returns: str representing the Checksum value returned from the SciHub API
    """
    try:
        response = requests.get(
            f"{COPERNICUS_CHECKSUM_URL}/odata/v1/Products({image_id})"
        )
        response.raise_for_status()
        checksums = response.json()["value"][0]["Checksum"]
        md5_object = [c for c in checksums if c["Algorithm"] == "MD5"][0]
        return md5_object["Value"]
    except Exception as ex:
        raise ChecksumRetrievalException(
            "There was an error retrieving the Checksum for Granule with id:"
            f" {image_id}. {ex}"
        ) from None


def download_file(
    image_checksum: str,
    image_id: str,
    image_filename: str,
    download_url: str,
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
    """
    session_maker = get_session_maker()

    with get_session(session_maker) as db:
        try:
            token = get_copernicus_token()
            session = requests.Session()
            session.headers.update({"Authorization": f"Bearer {token}"})
            with session.get(url=download_url, stream=True) as response:
                response.raise_for_status()

                aws_checksum = generate_aws_checksum(image_checksum)

                s3_client = get_s3_client()
                upload_bucket = os.environ["UPLOAD_BUCKET"]
                root, ext = os.path.splitext(image_filename)
                zip_key = f"{root}.zip"
                s3_client.put_object(
                    Body=response.raw.read(),
                    Bucket=upload_bucket,
                    Key=f"{zip_key}",
                    ContentMD5=aws_checksum,
                )

                granule = db.query(Granule).filter(Granule.id == image_id).first()
                granule.downloaded = True
                granule.checksum = image_checksum
                db.commit()
        except requests.RequestException as ex:
            raise FailedToDownloadFileException(
                "Requests exception thrown downloading granule with download_url:"
                f" {download_url}, exception was: {ex}"
            ) from None
        except client.ClientError as ex:
            raise FailedToUploadFileException(
                f"Boto3 Client Error raised when uploading file: {image_filename}"
                f" for granule with id: {image_id}, error was: {ex}"
            ) from None
        except SQLAlchemyError as ex:
            db.rollback()
            raise FailedToUpdateGranuleDownloadFinishException(
                "SQLAlchemy Exception raised when updating download finish for"
                f" granule with id: {image_id}, exception was: {ex}"
            ) from None


def get_s3_client() -> "S3Client":
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
    session_maker = get_session_maker()

    try:
        with get_session(session_maker) as db:
            if status := (
                db.query(Status)
                .filter(Status.key_name == "last_file_downloaded_time")
                .first()
            ):
                status.value = datetime.now()
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
