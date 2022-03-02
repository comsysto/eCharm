import logging
import logging.config
import os
import sys

from dotenv import load_dotenv

parent_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(parent_dir)

logging_conf_path = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "logging.conf")
)
logging.config.fileConfig(logging_conf_path)
log = logging.getLogger(__package__)

try:
    load_dotenv(verbose=True)
except Exception:
    log.info("No dotenv detected, ignoring")

db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "5432")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "postgres")
db_uri = (
    "postgresql://"
    + "postgres"
    + ":"
    + "postgres"
    + "@"
    + "localhost"
    + ":"
    + "54322"
    + "/"
    + "myDatabase"
)
