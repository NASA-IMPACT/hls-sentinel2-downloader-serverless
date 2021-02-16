import json
import os

import alembic.command
import alembic.config
import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_secretsmanager
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

UNIT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(UNIT_TEST_DIR, "docker-compose.yml")


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def postgres_engine(docker_ip, docker_services, db_connection_secret):
    db_url = url.URL(
        "postgresql",
        username=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        host="localhost",
        database=os.environ["PG_DB"],
    )
    pg_engine = create_engine(db_url)
    docker_services.wait_until_responsive(
        timeout=15.0, pause=1, check=lambda: check_pg_status(pg_engine)
    )

    alembic_root = UNIT_TEST_DIR.replace("layers/db/db/tests", "alembic_migration")
    alembic_config = alembic.config.Config(os.path.join(alembic_root, "alembic.ini"))
    alembic_config.set_main_option("script_location", alembic_root)
    alembic.command.upgrade(alembic_config, "head")

    return pg_engine


@pytest.fixture(autouse=True)
def aws_credentials(monkeysession):
    monkeysession.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeysession.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeysession.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeysession.setenv("AWS_SESSION_TOKEN", "testing")
    monkeysession.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="session")
def secrets_manager_client(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="session")
def db_connection_secret(secrets_manager_client, monkeysession):
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
def monkeysession(request):
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture
def db_session(postgres_engine):
    connection = postgres_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()
