import json
import os
from contextlib import contextmanager
from typing import Callable, Iterator

import boto3
from sqlalchemy import create_engine
from sqlalchemy.engine import url
from sqlalchemy.orm import Session, sessionmaker


def _get_url() -> url.URL:
    secrets_manager_client = boto3.client("secretsmanager")
    secret_arn = os.environ["DB_CONNECTION_SECRET_ARN"]
    db_connection_params = json.loads(
        secrets_manager_client.get_secret_value(SecretId=secret_arn)["SecretString"]
    )

    return url.URL.create(
        "postgresql",
        username=db_connection_params["username"],
        password=db_connection_params["password"],
        host=db_connection_params["host"],
        database=db_connection_params["dbname"],
    )


def get_session_maker() -> Callable[[], Session]:
    return sessionmaker(autocommit=False, bind=create_engine(_get_url()))


@contextmanager
def get_session(session_maker: Callable[[], Session]) -> Iterator[Session]:
    db = session_maker()

    try:
        yield db
    finally:
        db.close()
