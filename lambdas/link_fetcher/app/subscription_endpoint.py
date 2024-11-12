import logging
import json
import os
import secrets
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

import boto3
import iso8601
from app.common import (
    SearchResult,
    SessionMaker,
    add_search_results_to_db_and_sqs,
    filter_search_results,
    get_accepted_tile_ids,
    parse_tile_id_from_title,
)
from db.session import get_session_maker
from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.requests import Request

if TYPE_CHECKING:
    from mypy_boto3_ssm.client import SSMClient

logger = logging.getLogger(__name__)


@dataclass
class EndpointConfig:
    """Configuration settings for subscription 'push' endpoint"""

    stage: str = os.getenv("STAGE")

    # user auth
    notification_username: str = os.getenv("NOTIFICATION_USERNAME")
    notification_password: str = os.getenv("NOTIFICATION_PASSWORD")

    def __post_init__(self):
        for attr, value in asdict(self).items():
            if value is None:
                raise ValueError(f"EndpointConfig attribute '{attr}' must be defined (got None)")

    @classmethod
    def load_from_secrets_manager(cls, stage: str) -> "EndpointConfig":
        """Load from AWS Secret Manager for some `stage`"""
        try:
            secrets_manager_client = boto3.client("secretsmanager")
            secret = json.loads(
                secrets_manager_client.get_secret_value(
                    SecretId=(
                        f"hls-s2-downloader-serverless/{stage}/esa-subscription-credentials"
                    )
                )["SecretString"]
            )
        except Exception as ex:
            raise RuntimeError("Could not retrieve ESA subscription credentials from Secrets Manager") from ex
        return cls(
            notification_username=secret["notification_username"],
            notification_password=secret["notification_password"],
        )

    def get_endpoint_url(
        self,
        ssm_client: "SSMClient",
        param_name_template: str = "/integration_tests/{stage}/link_subscription_endpoint_url",
    ) -> str:
        """Return the endpoint URL stored in SSM parameter store"""
        param_name = param_name_template.format(stage=self.stage)
        result = ssm_client.get_parameter(Name=param_name)
        if (value := result["Parameter"].get("Value")) is None:
            raise ValueError(f"No such SSM parameter {param_name}")
        return value


def process_notification(
    notification: dict[str, Any],
    accepted_tile_ids: set[str],
    session_maker: SessionMaker,
):
    payload = notification["value"]

    extracted_links = [
        location
        for location in payload["Locations"]
        if location["FormatType"] == "Extracted"
    ]
    if len(extracted_links) != 1:
        raise ValueError(
            f"Got {len(extracted_links)} 'Extracted' links, expected just 1"
        )
    extracted = extracted_links[0]

    search_result = SearchResult(
        image_id=payload["Id"],
        filename=payload["Name"],
        tileid=parse_tile_id_from_title(payload["Name"]),
        size=payload["ContentLength"],
        beginposition=iso8601.parse_date(payload["ContentDate"]["Start"]),
        endposition=iso8601.parse_date(payload["ContentDate"]["End"]),
        ingestiondate=iso8601.parse_date(payload["PublicationDate"]),
        download_url=extracted["DownloadLink"],
    )

    filtered_search_result = filter_search_results(
        [search_result],
        accepted_tile_ids,
    )

    if filter_search_results:
        logger.info(f"Adding {search_result=} to granule download queue")
        add_search_results_to_db_and_sqs(
            session_maker,
            filtered_search_result,
        )
    else:
        logger.info(f"Rejected {search_result=} (unacceptable tile)")


def build_app(config: EndpointConfig) -> FastAPI:
    """Create FastAPI app"""
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    security = HTTPBasic()

    @app.post("/events", status_code=204)
    def post_notification(
        request: Request,
        notification: dict[str, Any],
        accepted_tile_ids: set[str] = Depends(get_accepted_tile_ids),
        credentials: HTTPBasicCredentials = Depends(security),
        session_maker: SessionMaker = Depends(get_session_maker),
    ) -> Response:
        """
        Endpoint which uses Basic Auth and processes acquired notification.
        """
        # check Basic authorization
        if not (
            secrets.compare_digest(
                credentials.username.encode("utf-8"),
                config.notification_username.encode("utf-8"),
            )
            and secrets.compare_digest(
                credentials.password.encode("utf-8"),
                config.notification_password.encode("utf-8"),
            )
        ):
            raise HTTPException(status_code=401, detail="Unauthorized")

        # process notification
        process_notification(
            notification=notification,
            accepted_tile_ids=accepted_tile_ids,
            session_maker=session_maker,
        )
        return Response(status_code=204)

    return app


if __name__ == "__main__":
    # for local dev
    import uvicorn

    config = EndpointConfig()
    app = build_app(config)
    uvicorn.run(app)
