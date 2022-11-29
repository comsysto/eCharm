import configparser
from typing import Dict, List, Optional

from difflib import SequenceMatcher
import geopandas as gpd
import pandas as pd
from tqdm import tqdm


class StationMerger:
    def __init__(self, config: configparser, con):
        self.config = config
        self.con = con



    def _determine_duplicates(
        self,
        current_station: pd.Series,
        duplicate_candidates: pd.DataFrame,
        score_threshold: float = 0.49,
        max_distance: int = 100,
        score_weights: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        score_weights = (
            score_weights
            if score_weights
            else dict(operator=0.2, address=0.1, distance=0.7)
        )

        duplicate_candidates["operator_match"] = duplicate_candidates.operator.apply(
            lambda x: SequenceMatcher(None, current_station.operator, str(x)).ratio()
            if (current_station.operator is not None) & (x is not None)
            else 0.0
        )

        current_station_address = f"{current_station['street']},{current_station['town']}"
        duplicate_candidates["address"] = duplicate_candidates[
            ["street", "town"]
        ].apply(lambda x: f"{x['street']},{x['town']}", axis=1)
        duplicate_candidates["address_match"] = duplicate_candidates.address.apply(
            lambda x: SequenceMatcher(None, current_station_address, x).ratio()
            if (current_station_address != "None,None") & (x != "None,None")
            else 0.0,
        )

        operator_score = (
            score_weights["operator"] * duplicate_candidates["operator_match"]
        )
        address_score = score_weights["address"] * duplicate_candidates["address_match"]
        distance_score = 70 # TODO check if we still need a distance score, wanted to filter hierarchically
        #score_weights["distance"] * (
        #    1 - duplicate_candidates["distance_meter"] / max_distance
        #)
        duplicate_candidates["matching_score"] = (
            operator_score + address_score + distance_score
        )
        duplicate_candidates.loc[
            (duplicate_candidates.matching_score > score_threshold), "is_duplicate"
        ] = True
        return duplicate_candidates.loc[duplicate_candidates.is_duplicate, :]


    def merge(self):
        '''
            Note: We should use geography types as it's much more accurate than projection from WSG-84 to Mercator
            E.g.
            select ST_Distance(ST_Transform('SRID=4326;POINT (11.5739817 48.1589335)'::geometry, 3857), 
                    ST_Transform('SRID=4326;POINT (11.5375666 48.1532363)'::geometry, 3857));
            --> 4163 meters 
            select ST_Distance('SRID=4326;POINT (11.5739817 48.1589335)'::geography, 
                            'SRID=4326;POINT (11.5375666 48.1532363)'::geography);
            --> 2782 meters (correct)
        '''

        find_surrounding_stations_sql = """ 
            SELECT 
                s.id as station_id,
                s.data_source, s.coordinates, s.operator,
                c.capacity,
                a.street, a.town 
            FROM stations s
            LEFT JOIN charging c ON s.id = c.station_id
            LEFT JOIN address a ON s.id = a.station_id 
            WHERE ST_Dwithin(ST_PointFromWkb(s.coordinates, 4326)::geography, 
                              ST_PointFromText('{center_coordinates}', 4326)::geography, 
                              {radius_m});
        """

        # First get list of stations esp. their coordinates
        gdf: gpd.GeoDataFrame = gpd.read_postgis("SELECT id as station_id, coordinates FROM stations",
                                                 con=self.con, geom_col="coordinates")
        # For each station's coordinate find all surrounding stations within a certain radius (including itself)
        radius_m = 100
        score_threshold: float = 0.49
        score_weights = dict(operator=0.2, address=0.1, distance=0.7)
        for idx in tqdm(range(gdf.shape[0])):
            current_station: pd.Series = gdf.iloc[idx]
            print(f"{current_station}")

            sql = find_surrounding_stations_sql.format(station_id=current_station['station_id'],
                                                       center_coordinates=current_station['coordinates'],
                                                       radius_m=radius_m)
            print(sql)
            nearby_stations: gpd.GeoDataFrame = gpd.read_postgis(sql, con=self.con, geom_col="coordinates")

            print(nearby_stations.columns)
            print(nearby_stations)
            if len(nearby_stations) < 2:
                # skip if only center station itself was found
                continue

            duplicates: pd.DataFrame = self._determine_duplicates(
                current_station=nearby_stations[nearby_stations['station_id'] == current_station['station_id']],
                duplicate_candidates=nearby_stations[nearby_stations['station_id'] != current_station['station_id']],
                score_threshold=score_threshold,
                max_distance=radius_m,
                score_weights=score_weights,
            )

            print(duplicates)
            break



