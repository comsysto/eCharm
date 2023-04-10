from geoalchemy2.types import Geography
from sqlalchemy import Column, Date, Integer, String, Boolean, Index
from sqlalchemy.orm import relationship

from charging_stations_pipelines.models import Base


class Station(Base):
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String, index=True, nullable=False, unique=True)
    data_source = Column(String)
    operator = Column(String)
    payment = Column(String)
    authentication = Column(String)
    point = Column(Geography(geometry_type='POINT', srid=4326))
    date_created = Column(Date)
    date_updated = Column(Date)
    raw_data = Column(String)
    country_code = Column(String)
    address = relationship("Address", back_populates="station", uselist=False)
    charging = relationship("Charging", back_populates="station", uselist=False)
    is_merged = Column(Boolean, default=False)
    merge_status = Column(String)


    def __repr__(self):
        return "<stations with id: {}>".format(self.id)


Index(
    "stations_point_geom_idx",
    Station.__table__.c.point,
    postgresql_using='gist',
)

"""
class MergeRelation(Base):
    __tablename__ = "merge_relations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stations_id = Column(Integer, ForeignKey("stations.id"))


class StationMerged(Station):
    __tablename__ = "merged_stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    # every merged station should have mapping to sources,
    # i.e. merge_relations 1:n mapping, maybe also with is_selected, is_duplicate otherwise extra columns for that in table?
    merge_relations = relationship("MergeRelation")

    __mapper_args__ = {
        "polymorphic_identity": "merged_stations",
        "concrete": True,
    }

"""