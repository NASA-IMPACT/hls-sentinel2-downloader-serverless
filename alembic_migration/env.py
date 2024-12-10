import json
import os
from logging.config import fileConfig

import boto3
from alembic import context
from db.models.base import Base
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import url

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url() -> url.URL:
    """
    Returns a SQLAlchemy `engine.url.URL`
    based on a AWS SecretsManager Secret, whose ARN is available as a environment
    variable named DB_CONNECTION_SECRET_ARN
    :returns: URL representing a sqlalchemy url for the database
    """
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


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
