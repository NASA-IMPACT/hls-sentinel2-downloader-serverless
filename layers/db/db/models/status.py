from sqlalchemy import Column, String

from .base import Base


class Status(Base):
    key_name = Column(String(length=256), primary_key=True)
    value = Column(String(length=256), nullable=False)
