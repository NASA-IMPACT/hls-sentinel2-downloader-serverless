import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session, sessionmaker

url = URL(
    "postgresql",
    username=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    host=os.environ.get("PG_HOST", "localhost"),
    database=os.environ["PG_DB"],
)

db_engine = create_engine(url)
DbSession = sessionmaker(autocommit=False, bind=db_engine)


@contextmanager
def get_session() -> Session:
    try:
        db = DbSession()
        yield db
    finally:
        db.close()
