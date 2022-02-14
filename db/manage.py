import os
import logging
from db.helper import is_db_healthy
import click
log = logging.getLogger('app')


@click.group()
def main():
    """Commands related to Database"""


@main.command('check_health')
def check_health():
    healthy = is_db_healthy()
    if healthy:
        log.info("DB is healthy.")
    else:
        log.critical("DB is not health.")


if __name__ == '__main__':
    main()
