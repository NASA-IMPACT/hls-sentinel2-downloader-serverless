#!/usr/bin/env python
import datetime as dt
import json
import os
import urllib.parse
from dataclasses import dataclass

import boto3
import requests

from app.subscription_endpoint import EndpointConfig


ACCESS_TOKEN_REQUEST_DATA: str = (
    "client_id={client_id}&"
    "username={user_email}&"
    "password={password}&"
    "grant_type=password&"
)

REFRESH_ACCESS_TOKEN_DATA: str = (
    "client_id={client_id}&"
    "refresh_token={refresh_token}&"
    "grant_type=refresh_token&"
)


@dataclass
class SubscriptionAPIConfig:
    client_id: str = os.getenv("ESA_CDSE_CLIENT_ID", "cdse-public")
    user_email: str = os.getenv("ESA_CDSE_USER_EMAIL")
    user_password: str = os.getenv("ESA_CDSE_USER_PASSWORD")

    identity_token_api_url: str = os.getenv(
        "ESA_CDSE_TOKEN_API_URL",
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
    )
    subscriptions_api_base_url: str = os.getenv(
        "ESA_CDSE_SUBSCRIPTION_API_BASE_URL",
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Subscriptions"
    )

    def __post_init__(self):
        if self.user_email is None:
            raise ValueError("Must set user_email")
        if self.user_password is None:
            raise ValueError("Must set user_password")


@dataclass
class Token:
    """ESA token"""

    access_token: str
    refresh_token: str
    expires_at: dt.datetime
    refresh_expires_at: dt.datetime

    @property
    def is_expired(self) -> bool:
        return dt.datetime.now() >= self.expires_at

    @property
    def is_refreshable(self) -> bool:
        return dt.datetime.now() >= self.refresh_expires_at


@dataclass
class TokenAPI:

    config: SubscriptionAPIConfig

    def get_access_token(self) -> Token:
        """
        Get access token which will be used in Subscriptions API.
        """
        # create data for request to get access token
        data = ACCESS_TOKEN_REQUEST_DATA.format(
            client_id=urllib.parse.quote(self.config.client_id),
            user_email=urllib.parse.quote(self.config.user_email),
            password=urllib.parse.quote(self.config.user_password),
        )

        # in headers we provide information in which format we send data
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # make request for access_token
        now = dt.datetime.now()
        response = requests.post(
            url=self.config.identity_token_api_url,
            headers=headers,
            data=data,
        )
        response.raise_for_status()

        # acquire access token and refresh token for future use
        response_json = json.loads(response.content.decode("utf-8"))

        return Token(
            access_token=response_json["access_token"],
            refresh_token=response_json["refresh_token"],
            expires_at=now + dt.timedelta(seconds=response_json["expires_in"]),
            refresh_expires_at=now + dt.timedelta(seconds=response_json["refresh_expires_in"]),
        )

    def refresh_token(self, token: Token) -> Token:
        """
        Refresh your access token.
        """
        # create data for request to get refresh access token
        data = REFRESH_ACCESS_TOKEN_DATA.format(
            client_id=urllib.parse.quote(self.config.client_id),
            refresh_token=urllib.parse.quote(token.refresh_token),
        )

        # in headers we provide information in which format we send data
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # make request for refreshing token
        now = dt.datetime.now()
        response = requests.post(
            url=self.config.identity_token_api_url,
            headers=headers,
            data=data,
        )
        response.raise_for_status()

        # acquire access token and refresh token for future use
        response_json = json.loads(response.content.decode("utf-8"))

        return Token(
            access_token=response_json["access_token"],
            refresh_token=response_json["refresh_token"],
            expires_at=now + dt.timedelta(seconds=response_json["expires_in"]),
            refresh_expires_at=now + dt.timedelta(seconds=response_json["refresh_expires_in"]),
        )


@dataclass
class SubscriptionAPI:
    """Create, list, and delete subscriptions"""

    token_api: TokenAPI
    endpoint_config: EndpointConfig

    def create_subscription(self) -> str:
        """
        Create example subscription, returning subscription ID
        """
        token = self.token_api.get_access_token()
        endpoint_url = self.endpoint_config.get_endpoint_url(ssm_client=boto3.client("ssm"))
        subscription_data = {
            "StageOrder": True,
            # example filter - You can adjust value to Your desired product type, collection etc.
            "FilterParam": "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq 'S2MSI1C')",
            "Priority": 1,
            "NotificationEndpoint": endpoint_url,
            "NotificationEpUsername": self.endpoint_config.notification_username,
            "NotificationEpPassword": self.endpoint_config.notification_password,
            "Status": "running",
            "SubscriptionEvent": ["created"],
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token.access_token}",
        }
        response = requests.post(
            url=self.token_api.config.subscriptions_api_base_url,
            headers=headers,
            json=subscription_data,
        )
        response.raise_for_status()
        subscription_information = response.json()
        subscription_id = subscription_information["Id"]
        print("Subscription created. Its ID is " + subscription_id + ".")
        print("Below is full response:")
        print(subscription_information)
        return subscription_id

    def list_subscriptions(self) -> list[dict]:
        """List subscriptions"""
        token = self.token_api.get_access_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token.access_token}",
        }
        response = requests.get(
            url=f"{self.token_api.config.subscriptions_api_base_url}/Info",
            headers=headers,
        )
        response.raise_for_status()
        subscriptions = response.json()
        return subscriptions

    def terminate_subscription(self, subscription_id: str):
        """
        Terminate test subscription.
        """
        token = self.token_api.get_access_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token.access_token}",
        }
        response = requests.delete(
            url=self.token_api.config.subscriptions_api_base_url + f"({subscription_id})",
            headers=headers,
        )
        response.raise_for_status()
        print("Subscription terminated. Its ID was " + subscription_id + ".")


if __name__ == "__main__":
    endpoint_cfg = EndpointConfig.load_from_secrets_manager(os.environ["IDENTIFIER"])
    subscription_cfg = SubscriptionAPIConfig()

    token_api = TokenAPI(subscription_cfg)

    subscription_api = SubscriptionAPI(token_api, endpoint_cfg)
    subscriptions = subscription_api.list_subscriptions()

    if not subscriptions:
        print("Creating new subscription")
        sub = subscription_api.create_subscription()
        print(sub)
    else:
        subscription_id = subscriptions[0]["Id"]
        print(f"Creating new subscription id={subscription_id}")
        subscription_api.terminate_subscription(subscription_id)

    print("DONE")
