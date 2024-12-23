import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import alembic.command
import alembic.config
import boto3
import pytest
from _pytest.monkeypatch import MonkeyPatch
from moto import mock_aws
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, url
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

DELETE_DATABASE_TABLE_CONTENTS = """
DELETE from granule;
DELETE from granule_count;
DELETE from status;
"""
UNIT_TEST_DIR = Path(__file__).absolute().parent


@pytest.fixture
def example_checksum_response() -> str:
    return json.loads((UNIT_TEST_DIR / "example_checksum_response.json").read_text())


@pytest.fixture
def checksum_file_contents() -> str:
    return (UNIT_TEST_DIR / "checksum_test_file.txt").read_text()


@pytest.fixture
def fake_safe_file_contents() -> bytes:
    return (UNIT_TEST_DIR / "fake_safe_file.SAFE").read_bytes()


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
def db_session(postgres_engine):
    session = Session(bind=postgres_engine, autocommit=False)
    yield session
    session.execute(DELETE_DATABASE_TABLE_CONTENTS)
    session.commit()
    session.close()


@pytest.fixture
def s3_resource():
    with mock_aws():
        yield boto3.resource("s3")


@pytest.fixture
def mock_s3_bucket(s3_resource, monkeysession):
    bucket = s3_resource.Bucket("test-bucket")
    bucket.create()
    monkeysession.setenv("UPLOAD_BUCKET", bucket.name)
    return bucket


@pytest.fixture(scope="session")
def secrets_manager_client():
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture(scope="session")
def ssm_client():
    with mock_aws():
        yield boto3.client("ssm")


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


@pytest.fixture(scope="session")
def mock_get_copernicus_token(ssm_client, monkeysession):
    token = "token"
    ssm_client.put_parameter(
        Name="/hls-s2-downloader-serverless/test/copernicus-token",
        Value=token,
        Type="String",
    )
    monkeysession.setenv("STAGE", "test")
    return token


def check_pg_status(engine: Engine) -> bool:
    try:
        engine.execute("SELECT * FROM pg_catalog.pg_tables;")
        return True
    except OperationalError:
        return False


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig) -> str:
    return str(UNIT_TEST_DIR / "docker-compose.yml")


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
