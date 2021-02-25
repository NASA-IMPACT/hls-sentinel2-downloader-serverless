from sqlalchemy import BigInteger, Column, Date, DateTime

from .base import Base


class GranuleCount(Base):
    date = Column(Date, primary_key=True)
    available_links = Column(BigInteger, nullable=False)
    fetched_links = Column(BigInteger, nullable=False)
    last_fetched_time = Column(DateTime, nullable=False)
