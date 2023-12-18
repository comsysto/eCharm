"""Handling of pipeline setup settings: environment variables, DB, logger."""
import configparser
import logging.config
import os
from typing import Final

from dotenv import load_dotenv

from charging_stations_pipelines import PROJ_CONFIG_DIR, PROJ_ROOT

logging.config.fileConfig(PROJ_ROOT / "logging.conf")

logger = logging.getLogger(__name__)


try:
    load_dotenv(verbose=True)
except Exception:
    logger.warning("No dotenv detected, ignoring")


def init_config() -> configparser:
    """Initializes the configuration from the config.ini file."""
    cfg: configparser = configparser.RawConfigParser()
    cfg.read(PROJ_CONFIG_DIR / "config.ini")
    return cfg


config: configparser = init_config()


restrict_tables: Final[bool] = os.getenv("DB_ALEMBIC_RESTRICT_TABLES", "false") == "true"

db_name: Final[str] = os.getenv("DB_NAME")
db_host: Final[str] = os.getenv("DB_HOST")
db_port: Final[str] = os.getenv("DB_PORT", "5432")
db_user: Final[str] = os.getenv("DB_USER", "postgres")
db_password: Final[str] = os.getenv("DB_PASSWORD", "postgres")
db_schema: Final[str] = os.getenv("DB_SCHEMA", "public")
db_table_prefix: Final[str] = os.getenv("DB_TABLE_PREFIX", "")

db_uri: Final[str] = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
