import json
import os
from contextlib import contextmanager
from unittest.mock import patch

import alembic.command
import alembic.config
import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_s3, mock_secretsmanager
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, url
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

DELETE_DATABASE_TABLE_CONTENTS = """
DELETE from granule; DELETE from granule_count; DELETE from status;
"""
UNIT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def example_checksum_response():
    with open(
        os.path.join(UNIT_TEST_DIR, "example_scihub_checksum_response.json"), "rb"
    ) as json_in:
        return json.load(json_in)


@pytest.fixture
def checksum_file_contents():
    with open(
        os.path.join(UNIT_TEST_DIR, "checksum_test_file.txt"), "rb"
    ) as checksum_in:
        return checksum_in.read()


@pytest.fixture
def fake_safe_file_contents():
    with open(os.path.join(UNIT_TEST_DIR, "fake_safe_file.SAFE"), "rb") as safe_in:
        return safe_in.read()


@pytest.fixture(autouse=True)
def aws_credentials(monkeysession):
    monkeysession.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeysession.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeysession.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeysession.setenv("AWS_SESSION_TOKEN", "testing")
    monkeysession.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="session")
def postgres_engine(docker_ip, docker_services, db_connection_secret):
    db_url = url.URL.create(
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

    alembic_root = UNIT_TEST_DIR.replace(
        "lambdas/downloader/tests", "alembic_migration"
    )
    alembic_config = alembic.config.Config(os.path.join(alembic_root, "alembic.ini"))
    alembic_config.set_main_option("script_location", alembic_root)
    alembic.command.upgrade(alembic_config, "head")

    return pg_engine


@pytest.fixture
def db_session(postgres_engine):
    session = Session(bind=postgres_engine, autocommit=False)
    yield session
    session.execute(DELETE_DATABASE_TABLE_CONTENTS)
    session.commit()
    session.close()


@pytest.fixture
def s3_resource():
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture
def mock_s3_bucket(s3_resource, monkeysession):
    bucket = s3_resource.Bucket("test-bucket")
    bucket.create()
    monkeysession.setenv("UPLOAD_BUCKET", bucket.name)
    return bucket


@pytest.fixture(scope="session")
def secrets_manager_client():
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
def mock_scihub_credentials(secrets_manager_client, monkeysession):
    secret = {"username": "test-username", "password": "test-password"}
    secrets_manager_client.create_secret(
        Name="hls-s2-downloader-serverless/test/scihub-credentials",
        SecretString=json.dumps(secret),
    )
    monkeysession.setenv("STAGE", "test")
    return secret


@pytest.fixture(scope="session")
def mock_inthub2_credentials(secrets_manager_client, monkeysession):
    secret = {"username": "test-inthub2-username", "password": "test-inthub2-password"}
    secrets_manager_client.create_secret(
        Name="hls-s2-downloader-serverless/test/inthub2-credentials",
        SecretString=json.dumps(secret),
    )
    monkeysession.setenv("STAGE", "test")
    return secret


@pytest.fixture(scope="session")
def mock_coperernicus_credentials(secrets_manager_client, monkeysession):
    secret = {
        "username": "test-copernicus-username",
        "password": "test-copernicus-password",
    }
    secrets_manager_client.create_secret(
        Name="hls-s2-downloader-serverless/test/copernicus-credentials",
        SecretString=json.dumps(secret),
    )
    monkeysession.setenv("STAGE", "test")
    return secret


@pytest.fixture()
def get_copernicus_token():
    with patch(
        "handler.get_copernicus_token", return_value="token", autospec=True
    ) as m:
        yield m


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(UNIT_TEST_DIR, "docker-compose.yml")


@pytest.fixture(scope="session")
def monkeysession(request):
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture
def fake_db_session_that_fails():
    class DB:
        def __init__(self):
            pass

        def query(self, something):
            raise SQLAlchemyError("An Exception")

        def rollback(self):
            pass

    @contextmanager
    def fake_session(session_maker):
        yield DB()

    yield fake_session


@pytest.fixture
def fake_db_session():
    class DB:
        def __init__(self):
            pass

        def rollback(self):
            pass

    @contextmanager
    def fake_session(session_maker):
        yield DB()

    yield fake_session
