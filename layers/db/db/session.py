import json
import os
from contextlib import contextmanager

import boto3
from sqlalchemy import create_engine
from sqlalchemy.engine import url
from sqlalchemy.orm import Session, sessionmaker


def _get_url():
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


def get_session_maker():
    url = _get_url()
    db_engine = create_engine(url)
    return sessionmaker(autocommit=False, bind=db_engine)


@contextmanager
def get_session(session_maker) -> Session:
    try:
        db = session_maker()
        yield db
    finally:
        db.close()
