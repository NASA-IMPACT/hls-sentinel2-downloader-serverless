"""initiation

Revision ID: 1c8c38951d47
Revises:
Create Date: 2021-01-14 12:30:09.347153

"""
from alembic import op
from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, SmallInteger, String

# revision identifiers, used by Alembic.
revision = "1c8c38951d47"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "granule",
        Column("id", String(length=256), primary_key=True),
        Column("filename", String(length=256), nullable=False),
        Column("tileid", String(length=5), nullable=False),
        Column("size", BigInteger, nullable=False),
        Column("beginposition", DateTime, nullable=False),
        Column("endposition", DateTime, nullable=False),
        Column("ingestiondate", DateTime, nullable=False),
        Column("download_url", String(256), nullable=False),
        Column("checksum", String(256), nullable=False, server_default=""),
        Column("downloaded", Boolean, nullable=False, server_default="false"),
        Column("download_started", DateTime, server_default=None),
        Column("download_finished", DateTime, server_default=None),
        Column("download_retries", SmallInteger, nullable=False, server_default="0"),
        Column("expired", Boolean, nullable=False, server_default="false"),
    )

    op.create_table(
        "granule_count",
        Column("date", Date, primary_key=True),
        Column("available_links", BigInteger, nullable=False),
        Column("fetched_links", BigInteger, nullable=False),
        Column("last_fetched_time", DateTime, nullable=False),
    )

    op.create_table(
        "status",
        Column("key_name", String(length=256), primary_key=True),
        Column("value", String(length=256), nullable=False),
    )


def downgrade():
    op.drop_table("granule")
    op.drop_table("granule_count")
    op.drop_table("status")
