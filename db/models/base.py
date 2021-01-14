from re import sub

from sqlalchemy.ext.declarative import declarative_base, declared_attr


class CustomBase:
    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(self):
        # Convert from CamelCase to lowercase snake_case
        # https://stackoverflow.com/a/1176023/728583
        return sub("(?!^)([A-Z]+)", r"_\1", self.__name__).lower()


Base = declarative_base(cls=CustomBase)
