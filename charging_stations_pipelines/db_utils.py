"""Module for database utilities."""
import logging

from sqlalchemy import delete, text, update
from sqlalchemy.orm import Session

from charging_stations_pipelines.models import address, charging, station

logger = logging.getLogger(__name__)


def delete_all_data(session: Session):
    """Deletes all data from the database."""
    logger.info("Deleting all data from the database...")
    models = [
        station.MergedStationSource,
        station.Station,
        address.Address,
        charging.Charging,
    ]
    for model in models:
        logger.info(f"Dropping table: '{model.__tablename__}'...")
        session.execute(
            text(f"TRUNCATE TABLE {model.__tablename__} RESTART IDENTITY CASCADE")
        )
    session.commit()
    session.close()
    logger.info("Finished deleting all data from the database.")


def delete_all_merged_data(session: Session):
    """Deletes all merged data from the database."""
    session.execute(
        text(
            f"TRUNCATE TABLE {station.MergedStationSource.__tablename__} RESTART IDENTITY"
        )
    )
    session.execute(delete(address.Address).where(address.Address.is_merged.is_(True)))
    session.execute(
        delete(charging.Charging).where(charging.Charging.is_merged.is_(True))
    )
    session.execute(delete(station.Station).where(station.Station.is_merged.is_(True)))
    session.execute(update(station.Station).values(merge_status=None))
    session.commit()
    session.close()
