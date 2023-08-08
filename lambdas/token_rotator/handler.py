import json
import logging
import os
from typing import TypedDict

import boto3
import requests

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

COPERNICUS_IDENTITY_URL = os.environ.get(
    "COPERNICUS_IDENTITY_URL",
    "https://identity.dataspace.copernicus.eu/auth/realms/"
    "CDSE/protocol/openid-connect/token"
)


class CopernicusAuthenticationNotRetrievedException(Exception):
    pass


class CopernicusTokenNotRetrievedException(Exception):
    pass


class CopernicusTokenNotWrittenException(Exception):
    pass


class CopernicusCredentials(TypedDict):
    username: str
    password: str


def get_copernicus_credentials() -> CopernicusCredentials:
    """
    Retrieves the username and password for Copernicus which are stored in
    SecretsManager
    :returns: CopernicusCredentials
    """
    try:
        stage = os.environ["STAGE"]
        secrets_manager_client = boto3.client("secretsmanager")
        secret = json.loads(
            secrets_manager_client.get_secret_value(
                SecretId=(
                    f"hls-s2-downloader-serverless/{stage}/copernicus-credentials"
                )
            )["SecretString"]
        )
        return {"username": secret["username"], "password": secret["password"]}
    except Exception as ex:
        raise CopernicusAuthenticationNotRetrievedException(
            f"There was an error retrieving Copernicus Credentials: {str(ex)}"
        )


def get_copernicus_token() -> str:
    """
    Retrieves keycloak token from Copernicus endpoint
    :returns: str of token
    """
    try:
        credentials = get_copernicus_credentials()
        data = {
            "client_id": "cdse-public",
            "username": credentials["username"],
            "password": credentials["password"],
            "grant_type": "password",
        }
        response = requests.post(COPERNICUS_IDENTITY_URL, data)
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as ex:
        error_message = (
            "There was error retrieving the keycloak token"
            f" {str(ex)}"
        )
        LOGGER.error(error_message)
        raise CopernicusTokenNotRetrievedException(error_message)


def handler(event, context):
    try:
        stage = os.environ["STAGE"]
        token = get_copernicus_token()
        ssm_client = boto3.client("ssm")
        ssm_client.put_parameter(
            Name=f"/hls-s2-downloader-serverless/{stage}/copernicus-token",
            Overwrite=True,
            Value=token,
        )
    except Exception as ex:
        error_message = (
            "There was an error writing the keycloak token"
            f" {str(ex)}"
        )
        LOGGER.error(error_message)
        raise CopernicusTokenNotWrittenException(error_message)
