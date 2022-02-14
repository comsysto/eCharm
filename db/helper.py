"""
  -----please use pep8 as a file guidline------
  __@project__: data_handler
  __@file name__: db.py
  __@author__: cohn
  __@date__: 08.06.20
"""
import logging

from sqlalchemy import (Boolean, Column, Date, ForeignKey, Integer, Numeric,
                        String, Table, create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.scoping import ScopedSession
from settings import db_uri
from db import engine

log = logging.getLogger(__package__)


def execute_sql(sql_command: str):
    connection = engine.connect()
    transaction = connection.begin()
    result = connection.execute(sql_command)
    transaction.commit()
    return result


def is_db_healthy():
    log.info("Checking DB health....")
    try:
        execute_sql("SELECT current_database()")
        print(execute_sql("SELECT current_database()"))
        log.debug("db is ok")
        return True
    except Exception as ex:
        log.exception("DB is unhealthy", exc_info=True)
        raise ex
