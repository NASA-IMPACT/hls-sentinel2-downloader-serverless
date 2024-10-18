import json
import os
import pathlib
from contextlib import contextmanager
from typing import cast

import alembic.command
import alembic.config
import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_aws
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from mypy_boto3_sqs.service_resource import SQSServiceResource
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.engine import Engine, Transaction, url  # type: ignore
from sqlalchemy.exc import OperationalError  # type: ignore
from sqlalchemy.orm import Session  # type: ignore

UNIT_TEST_DIR = pathlib.Path(__file__).parent


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def postgres_engine(docker_ip, docker_services, db_connection_secret: None):
    db_url = url.URL.create(
        "postgresql",
        username=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        host="localhost",
        database=os.environ["PG_DB"],
    )
    pg_engine = cast(Engine, create_engine(db_url))
    docker_services.wait_until_responsive(
        timeout=15.0, pause=1, check=lambda: check_pg_status(pg_engine)
    )

    alembic_root = UNIT_TEST_DIR.parent.parent.parent / "alembic_migration"
    alembic_config = alembic.config.Config(str(alembic_root / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(alembic_root))
    alembic.command.upgrade(alembic_config, "head")

    return pg_engine


@pytest.fixture
def db_session(postgres_engine: Engine):
    with postgres_engine.connect() as connection:
        with cast(Transaction, connection.begin()) as transaction:
            with Session(bind=connection) as session:
                yield session
            transaction.rollback()


@pytest.fixture(autouse=True)
def aws_credentials(monkeysession: MonkeyPatch):
    monkeysession.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeysession.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeysession.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeysession.setenv("AWS_SESSION_TOKEN", "testing")
    monkeysession.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def sqs_resource():
    with mock_aws():
        yield boto3.resource("sqs")


@pytest.fixture
def sqs_client():
    with mock_aws():
        yield boto3.client("sqs")


@pytest.fixture
def sqs_queue(sqs_resource: SQSServiceResource):
    return sqs_resource.create_queue(QueueName="mock-queue")


@pytest.fixture(scope="session")
def secrets_manager_client():
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="session")
def db_connection_secret(
    secrets_manager_client: SecretsManagerClient, monkeysession: MonkeyPatch
):
    arn = secrets_manager_client.create_secret(
        Name="db-connection",
        SecretString=json.dumps(
            {
                "username": os.environ["PG_USER"],
                "password": os.environ["PG_PASSWORD"],
                "host": "localhost",
                "dbname": os.environ["PG_DB"],
            }
        ),
    )["ARN"]
    monkeysession.setenv("DB_CONNECTION_SECRET_ARN", arn)


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig: pytest.Config):
    return UNIT_TEST_DIR / "docker-compose.yml"


@pytest.fixture(scope="session")
def monkeysession(request: pytest.FixtureRequest):
    with MonkeyPatch().context() as mp:
        yield mp
