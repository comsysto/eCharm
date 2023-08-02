from dataclasses import dataclass

from shapely.speedups._speedups import Point


@dataclass(frozen=True)
class Station:
    identifier: int
    data_source: str
    operator: str
    point: Point
    address: str

    def __eq__(self, other):
        return self.identifier == other.identifier

    def __hash__(self):
        return hash(self.identifier)

    def __str__(self):
        return f"Station(id={self.identifier}, data_source={self.data_source}, operator={self.operator}, point={self.point}, address={self.address})"


@dataclass(frozen=True)
class StationDuplicate:
    station: Station
    duplicate: Station
    delta: float
