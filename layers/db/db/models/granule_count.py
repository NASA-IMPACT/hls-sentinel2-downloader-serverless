from sqlalchemy import BigInteger, Column, Date, DateTime, String
from sqlalchemy.schema import PrimaryKeyConstraint

from .base import Base


class GranuleCount(Base):
    date = Column(Date, primary_key=True)
    available_links = Column(BigInteger, nullable=False)
    fetched_links = Column(BigInteger, nullable=False)
    last_fetched_time = Column(DateTime, nullable=False)
    platform = Column(String, primary_key=True, server_default="S2A+S2B")

    __table_args__ = (
        PrimaryKeyConstraint("date", "platform", name="granule_count_pkey"),
    )
