from sqlalchemy import delete, text, update
from sqlalchemy.orm import Session

from charging_stations_pipelines import settings
from charging_stations_pipelines.models import address, charging, station


def delete_all_data(session: Session):
    session.execute(delete(station.MergedStationSource))
    session.execute(delete(address.Address))
    session.execute(delete(charging.Charging))
    session.execute(delete(station.Station))
    session.commit()
    session.close()


def delete_all_merged_data(session: Session):
    session.execute(delete(station.MergedStationSource))
    session.execute(delete(address.Address).where(address.Address.is_merged==True))
    session.execute(delete(charging.Charging).where(charging.Charging.is_merged==True))
    session.execute(delete(station.Station).where(station.Station.is_merged==True))
    session.execute(update(station.Station).values(merge_status=None))
    session.commit()
    session.close()



