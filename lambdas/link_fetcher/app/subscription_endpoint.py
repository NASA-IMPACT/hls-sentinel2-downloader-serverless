import json
import logging
import os
import secrets
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urljoin

import boto3
import iso8601
from db.session import get_session_maker
from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.requests import Request

from app.common import (
    SearchResult,
    SessionMaker,
    add_search_results_to_db_and_sqs,
    filter_search_results,
    get_accepted_tile_ids,
    parse_tile_id_from_title,
)

if TYPE_CHECKING:
    from mypy_boto3_ssm.client import SSMClient

logger = logging.getLogger(__name__)


@dataclass
class EndpointConfig:
    """Configuration settings for subscription 'push' endpoint"""

    stage: str = field(default_factory=lambda: os.getenv("STAGE"))

    # user auth
    notification_username: str = field(
        default_factory=lambda: os.getenv("NOTIFICATION_USERNAME")
    )
    notification_password: str = field(
        default_factory=lambda: os.getenv("NOTIFICATION_PASSWORD")
    )

    def __post_init__(self):
        for attr, value in asdict(self).items():
            if value is None:
                raise ValueError(
                    f"EndpointConfig attribute '{attr}' must be defined (got None)"
                )

    @classmethod
    def load_from_secrets_manager(cls, stage: str) -> "EndpointConfig":
        """Load from AWS Secret Manager for some `stage`"""
        secret_id = f"hls-s2-downloader-serverless/{stage}/esa-subscription-credentials"
        try:
            secrets_manager_client = boto3.client("secretsmanager")
            secret = json.loads(
                secrets_manager_client.get_secret_value(
                    SecretId=secret_id,
                )["SecretString"]
            )
        except Exception as ex:
            raise RuntimeError(
                "Could not retrieve ESA subscription credentials from Secrets Manager"
            ) from ex

        return cls(
            stage=stage,
            notification_username=secret["notification_username"],
            notification_password=secret["notification_password"],
        )

    def get_endpoint_url(
        self,
        ssm_client: "SSMClient",
    ) -> str:
        """Return the endpoint URL stored in SSM parameter store"""
        param_name = (
            f"/hls-s2-downloader-serverless/{self.stage}/link_subscription_endpoint_url"
        )
        result = ssm_client.get_parameter(Name=param_name)
        if (url := result["Parameter"].get("Value")) is None:
            raise ValueError(f"No such SSM parameter {param_name}")
        return urljoin(url, "events")


def parse_search_result(
    payload: dict,
) -> SearchResult:
    """Parse a subscription event payload to a SearchResult"""
    # There should only be 1 link to "extracted" data file
    extracted_links = [
        location
        for location in payload["Locations"]
        if location["FormatType"] == "Extracted"
    ]
    if len(extracted_links) != 1:
        raise ValueError(
            f"Got {len(extracted_links)} 'Extracted' links, expected just 1"
        )

    # The "extracted" data information looks like,
    # * FormatType: "Extracted"
    # * DownloadLink: str
    # * ContentLength: int
    # * Checksum: [{ "Value": str, "Algorithm": "MD5" | "BLAKE3", "ChecksumDate": datetime}, ...]
    # * S3Path: str
    extracted = extracted_links[0]

    # grab MD5 checksum
    checksum = [
        checksum["Value"]
        for checksum in extracted["Checksum"]
        if checksum["Algorithm"] == "MD5"
    ][0]

    search_result = SearchResult(
        image_id=payload["Id"],
        filename=payload["Name"],
        tileid=parse_tile_id_from_title(payload["Name"]),
        size=extracted["ContentLength"],
        beginposition=iso8601.parse_date(payload["ContentDate"]["Start"]),
        endposition=iso8601.parse_date(payload["ContentDate"]["End"]),
        ingestiondate=iso8601.parse_date(payload["PublicationDate"]),
        download_url=extracted["DownloadLink"],
        checksum=checksum,
    )
    return search_result


def process_notification(
    notification: dict[str, Any],
    accepted_tile_ids: set[str],
    session_maker: SessionMaker,
    now_utc: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
):
    """Parse, filter, and potentially add new granule results to download queue"""
    # Parse subscription notification to SearchResult
    search_result = parse_search_result(notification["value"])

    # Only consider imagery acquired in the last 30 days to avoid reprocessing of older imagery
    oldest_acquisition_date = now_utc() - timedelta(days=30)
    if search_result.beginposition < oldest_acquisition_date:
        logger.info(f"Rejected {search_result=} (acquisition date too old)")
        return

    # Check tile ID
    accepted_search_results = filter_search_results(
        [search_result],
        accepted_tile_ids,
    )
    if accepted_search_results:
        logger.info(f"Adding {search_result=} to granule download queue")
        add_search_results_to_db_and_sqs(
            session_maker,
            accepted_search_results,
        )
    else:
        logger.info(f"Rejected {search_result=} (unacceptable tile)")


def build_app(
    config: EndpointConfig,
    now_utc: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
) -> FastAPI:
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

    accepted_tile_ids = get_accepted_tile_ids()

    @app.post("/events", status_code=204)
    def post_notification(
        request: Request,
        notification: dict[str, Any],
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
            logging.error("Unauthorized")
            raise HTTPException(status_code=401, detail="Unauthorized")

        process_notification(
            notification=notification,
            accepted_tile_ids=accepted_tile_ids,
            session_maker=session_maker,
            now_utc=now_utc,
        )
        return Response(status_code=204)

    return app


if __name__ == "__main__":
    # for local dev
    import uvicorn

    config = EndpointConfig()
    app = build_app(config)
    uvicorn.run(app)
