from sqlalchemy import BigInteger, Boolean, Column, DateTime, SmallInteger, String

from .base import Base


class Granule(Base):
    id = Column(String(length=256), primary_key=True)
    filename = Column(String(length=256), nullable=False)
    tileid = Column(String(length=5), nullable=False)
    size = Column(BigInteger, nullable=False)
    checksum = Column(String(256), nullable=False, default="")
    beginposition = Column(DateTime, nullable=False)
    endposition = Column(DateTime, nullable=False)
    ingestiondate = Column(DateTime, nullable=False)
    download_url = Column(String(256), nullable=False)
    downloaded = Column(Boolean, nullable=False, default=False)
    in_progress = Column(Boolean, nullable=False, default=False)
    uploaded = Column(Boolean, nullable=False, default=False)
    retry = Column(SmallInteger, nullable=False, default=0)
    download_failed = Column(Boolean, nullable=False, default=False)
    expired = Column(Boolean, nullable=False, default=False)
    download_started = Column(DateTime, default=None)
    download_finished = Column(DateTime, default=None)
