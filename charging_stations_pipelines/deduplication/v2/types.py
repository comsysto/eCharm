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