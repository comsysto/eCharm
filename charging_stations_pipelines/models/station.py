"""Station database entity."""
from geoalchemy2 import Geography
from sqlalchemy import Boolean, Column, Date, ForeignKey, Index, Integer, JSON, String
from sqlalchemy.orm import relationship

from charging_stations_pipelines import settings
from charging_stations_pipelines.models import Base


class Station(Base):
    """Station class for representing a station in a database."""
    __tablename__ = f"{settings.db_table_prefix}stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String, index=True, nullable=True, unique=True)
    evse_country_id = Column(String)
    evse_operator_id = Column(String)
    evse_station_id = Column(String)
    data_source = Column(String)
    operator = Column(String)
    payment = Column(String)
    authentication = Column(String)
    point = Column(Geography(geometry_type='POINT', srid=4326))
    date_created = Column(Date)
    date_updated = Column(Date)
    raw_data = Column(JSON)
    country_code = Column(String)
    address = relationship("Address", back_populates="station", uselist=False)
    charging = relationship("Charging", back_populates="station", uselist=False)
    is_merged = Column(Boolean, default=False)
    merge_status = Column(String)
    source_stations = relationship("MergedStationSource")


Index(
    "stations_point_geom_idx",
    Station.__table__.c.point,
    postgresql_using="gist",
)


class MergedStationSource(Base):
    """This class represents a merged station source entity."""
    __tablename__ = f"{settings.db_table_prefix}merged_station_source"
    id = Column(Integer, primary_key=True, autoincrement=True)
    merged_station_id = Column(Integer, ForeignKey(f"{Station.__tablename__}.id"))
    duplicate_source_id = Column(String)
