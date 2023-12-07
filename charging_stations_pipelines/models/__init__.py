from typing import Any

from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from charging_stations_pipelines import settings


class BaseWithSafeSetProperty:
    def __setattr__(self, name: str, value: Any) -> None:
        """This method sets the value of the specified attribute and prevents accidental usage of non-existing class
            attributes.
        **Note:**: this method ignores internal object attributes (i.e. all attributes starting with an underscore).

        :param name: The name of the attribute to be set.
        :param value: The value to be assigned to the attribute.
        :raise AttributeError: If the attribute does not exist.
        :return: None.
        """
        if not (name.startswith("_") or hasattr(self, name)):
            raise AttributeError(f"Cannot set non-existing attribute '{name}' on class '{self.__class__.__name__}'.")
        super().__setattr__(name, value)

    def __repr__(self):
        return f"<{self.__class__.__name__} with id: {self.id}>"


Base = declarative_base(cls=BaseWithSafeSetProperty, metadata=MetaData(schema=settings.db_schema))
