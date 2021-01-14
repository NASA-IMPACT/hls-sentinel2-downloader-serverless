"""initiation

Revision ID: 1c8c38951d47
Revises:
Create Date: 2021-01-14 12:30:09.347153

"""
from sqlalchemy import BigInteger, Boolean, Column, DateTime, SmallInteger, String

from alembic import op

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
        Column("checksum", String(256), nullable=False),
        Column("beginposition", DateTime, nullable=False),
        Column("endposition", DateTime, nullable=False),
        Column("ingestiondate", DateTime, nullable=False),
        Column("download_url", String(256), nullable=False),
        Column("downloaded", Boolean, nullable=False, server_default="false"),
        Column("in_progress", Boolean, nullable=False, server_default="false"),
        Column("uploaded", Boolean, nullable=False, server_default="false"),
        Column("retry", SmallInteger, nullable=False, server_default="0"),
        Column("download_failed", Boolean, nullable=False, server_default="false"),
        Column("expired", Boolean, nullable=False, server_default="false"),
        Column("download_started", DateTime, server_default=None),
        Column("download_finished", DateTime, server_default=None),
    )


def downgrade():
    op.drop_table("granule")
