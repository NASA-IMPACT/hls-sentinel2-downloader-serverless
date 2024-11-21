import json
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
import httpx
import pytest
from db.models.granule import Granule
from fastapi import FastAPI
from freezegun import freeze_time
from moto import mock_aws
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

import app.subscription_endpoint
from app.common import SearchResult
from app.subscription_endpoint import (
    EndpointConfig,
    build_app,
    parse_search_result,
    process_notification,
)


class TestEndpointConfig:
    """Test EndpointConfig"""

    def test_basic_init_ennvar(self, monkeypatch):
        monkeypatch.setenv("STAGE", "local")
        monkeypatch.setenv("NOTIFICATION_USERNAME", "bar")
        monkeypatch.setenv("NOTIFICATION_PASSWORD", "baz")
        config = EndpointConfig()
        assert config.stage == "local"
        assert config.notification_username == "bar"
        assert config.notification_password == "baz"

    @pytest.fixture
    def endpoint_config_secret(self) -> EndpointConfig:
        config = EndpointConfig(
            stage="local",
            notification_username="bar",
            notification_password="baz",
        )

        with mock_aws():
            secrets_manager_client = boto3.client("secretsmanager")
            secrets_manager_client.create_secret(
                Name=f"hls-s2-downloader-serverless/{config.stage}/esa-subscription-credentials",
                SecretString=json.dumps(
                    {
                        "notification_username": config.notification_username,
                        "notification_password": config.notification_password,
                    }
                ),
            )
            yield config

    def test_local_from_ssm(self, monkeypatch, endpoint_config_secret: EndpointConfig):
        monkeypatch.setenv("STAGE", endpoint_config_secret.stage)
        config = EndpointConfig.load_from_secrets_manager(endpoint_config_secret.stage)
        assert config == endpoint_config_secret


@pytest.fixture
def event_s2_created() -> dict:
    """Load Sentinel-2 "Created" event from ESA's push subscription

    This message contains two types of fields,
    * Message metadata (event type, subscription ID, ack ID, notification date, etc)
    * Message "body" - `(.value)`
    """
    data = Path(__file__).parent / "data" / "push-granule-created-s2-n1.json"
    with data.open() as src:
        return json.load(src)


@pytest.fixture
def recent_event_s2_created(event_s2_created: dict) -> dict:
    """A "recent" Sentinel-2 "Created" event from ESA's push subscription

    We freeze time
    """
    # freeze to same day as publication date
    with freeze_time(event_s2_created["value"]["PublicationDate"]):
        yield event_s2_created


class TestSearchResultParsing:
    """Tests for parsing subscription into a SearchResult"""

    def test_parses_created_event(self, event_s2_created: dict):
        """Test happy path of parsing event to SearchResult"""
        search_result = parse_search_result(event_s2_created["value"])
        assert isinstance(search_result, SearchResult)

    def test_raises_if_no_extracted_data(self, event_s2_created: dict):
        """Test we catch if there's no "extracted" data"""
        # this should never happen as newly published data because it'll be "Online"
        payload = event_s2_created["value"]
        payload["Locations"][0]["FormatType"] = "Archived"

        with pytest.raises(
            ValueError, match=r"Got 0 'Extracted' links, expected just 1"
        ):
            parse_search_result(payload)

    def test_raises_if_multiple_extracted_data(self, event_s2_created: dict):
        """Test we catch if there's >1 "extracted" data"""
        # this also shouldn't happen, but if it does we want to fail because
        # it won't be clear which to download
        payload = event_s2_created["value"]
        payload["Locations"].append(payload["Locations"][0].copy())

        with pytest.raises(
            ValueError, match=r"Got 2 'Extracted' links, expected just 1"
        ):
            parse_search_result(payload)


class TestProcessNotification:
    """Test parsing and filtering of granule created notification"""

    def test_processes_notification(
        self,
        mock_sqs_queue,
        db_session: Session,
        recent_event_s2_created: dict,
        accepted_tile_ids: set[str],
    ):
        """Test that a recent S2 granule created event is added to queue"""
        process_notification(
            recent_event_s2_created,
            accepted_tile_ids,
            lambda: db_session,
        )

        assert len(db_session.query(Granule).all()) == 1

        mock_sqs_queue.load()
        number_of_messages_in_queue = mock_sqs_queue.attributes[
            "ApproximateNumberOfMessages"
        ]
        assert int(number_of_messages_in_queue) == 1

    def test_filters_old_imagery(
        self,
        mock_sqs_queue,
        db_session: Session,
        event_s2_created: dict,
        accepted_tile_ids: set[str],
    ):
        """Test we filter old imagery and do NOT add to queue or DB"""
        event_s2_created["value"]["ContentDate"]["Start"] = "1999-12-31T23:59:59.999Z"
        with patch(
            "app.subscription_endpoint.add_search_results_to_db_and_sqs"
        ) as mock_add_to_db_and_sqs:
            process_notification(
                event_s2_created,
                accepted_tile_ids,
                lambda: db_session,
            )
        mock_add_to_db_and_sqs.assert_not_called()

    def test_filters_unaccepted_tile_id(
        self,
        mock_sqs_queue,
        db_session: Session,
        recent_event_s2_created: dict,
        mocker,
    ):
        """Test we filter old imagery and do NOT add to queue or DB"""
        spy_filter_search_results = mocker.spy(
            app.subscription_endpoint, "filter_search_results"
        )
        with patch(
            "app.common.add_search_results_to_db_and_sqs"
        ) as mock_add_to_db_and_sqs:
            process_notification(
                recent_event_s2_created,
                {"none"},
                lambda: db_session,
            )
        spy_filter_search_results.assert_called_once()
        mock_add_to_db_and_sqs.assert_not_called()


class TestApp:
    """Test API endpoint"""

    @pytest.fixture()
    def config(self) -> EndpointConfig:
        return EndpointConfig(
            stage="local",
            notification_username="test_user",
            notification_password="password",
        )

    @pytest.fixture
    def app(
        self, config: EndpointConfig, db_connection_secret, mock_sqs_queue
    ) -> FastAPI:
        self.endpoint_config = config
        self.db_connection_secret = db_connection_secret
        self.mock_sqs_queue = mock_sqs_queue
        return build_app(config)

    @pytest.fixture
    def test_client(self, app: FastAPI) -> TestClient:
        return TestClient(app)

    def test_handles_new_created_event(
        self, test_client: TestClient, event_s2_created: dict
    ):
        """Test happy path for handling subscription event, mocking processing function"""
        with patch(
            "app.subscription_endpoint.process_notification", Mock()
        ) as mock_process_notification:
            resp = test_client.post(
                "/events",
                json=event_s2_created,
                auth=(
                    self.endpoint_config.notification_username,
                    self.endpoint_config.notification_password,
                ),
            )
        # processed successfully but no content
        resp.raise_for_status()
        assert resp.status_code == 204
        mock_process_notification.assert_called_once()

    def test_handles_new_created_event_is_added(
        self,
        test_client: TestClient,
        db_session: Session,
        recent_event_s2_created: dict,
    ):
        """Test happy path for handling subscription event, mocking DB and SQS

        This is also distinguished by using a "recent" event notification that works
        by freezing/rewinding time to acquisition date for the test duration.
        """
        resp = test_client.post(
            "/events",
            json=recent_event_s2_created,
            auth=(
                self.endpoint_config.notification_username,
                self.endpoint_config.notification_password,
            ),
        )

        # processed successfully but no content
        resp.raise_for_status()
        assert resp.status_code == 204

        # Check we have a message in moto's SQS queue
        self.mock_sqs_queue.load()
        number_of_messages_in_queue = int(
            self.mock_sqs_queue.attributes["ApproximateNumberOfMessages"]
        )
        assert number_of_messages_in_queue == 1

    def test_handles_wrong_user(self, test_client: TestClient, event_s2_created: dict):
        """Test happy path for handling subscription event"""
        resp = test_client.post(
            "/events",
            json=event_s2_created,
            auth=(
                "wrong",
                self.endpoint_config.notification_password,
            ),
        )
        with pytest.raises(httpx.HTTPStatusError) as err:
            resp.raise_for_status()
        assert err.value.response.status_code == 401  # unauthorized

    def test_handles_wrong_pass(self, test_client: TestClient, event_s2_created: dict):
        """Test happy path for handling subscription event"""
        resp = test_client.post(
            "/events",
            json=event_s2_created,
            auth=(
                self.endpoint_config.notification_username,
                "wrong",
            ),
        )
        with pytest.raises(httpx.HTTPStatusError) as err:
            resp.raise_for_status()
        assert err.value.response.status_code == 401  # unauthorized
