import configparser

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from deduplication import attribute_match_thresholds_strategy

class StationMerger:
    def __init__(self, config: configparser, con, is_test: bool = False):
        self.config = config
        self.con = con
        self.is_test = is_test


    def merge_attributes(self, station: pd.Series, duplicates_to_merge: pd.DataFrame):
        """
        Might be used in the future.

        :param station:
        :param duplicates_to_merge:
        :return:
        """
        for att_name in [
            "amperage",
            "operator",
            "payment",
            "socket_type",
            "authentication",
            "capacity",
            "voltage",
        ]:
            att_values = duplicates_to_merge[att_name].dropna().unique().tolist()
            att_values = [str(x) for x in att_values if len(str(x)) > 0]
            if att_name in station.dropna():
                att_value = str(station[att_name])
                att_values += (
                    [att_value] if ";" not in att_value else att_value.split(";")
                )
            att_values = set(att_values)
            new_value = (
                ";".join([str(x) for x in att_values]) if att_values else None
            )
            station.at[att_name] = new_value
        station.at["merged_attributes"] = True
        return

    def _merge_duplicates(self, current_station: pd.Series, duplicates: pd.DataFrame) -> pd.Series:
        """
        Simple procedure, which follows BNA > OCM > OSM.

        :param current_station: pd.Series contianing current station.
        :param duplicates: pd.DataFrame containing all duplicates.
        :return:
        """

        if current_station["data_source"] == "BNA":
            # in case of BNA vs OCM / OSM we always stick with BNA (we do not need to do anything)
            # TODO: check for missing attributes and merge in smart way
            return current_station
        data_sources = ["BNA", "OCM", "OSM"]
        # generate data source pairs following preference ordering: BNA > OCM > OSM
        ordered_data_source_pairs = [
            (x, y) for x in data_sources for y in data_sources if x != "BNA"
        ]
        selected_station: pd.Series = pd.Series()
        for (current_data_source, duplicate_data_source) in ordered_data_source_pairs:
            if current_station["data_source"] != current_data_source:
                continue
            mergeable_stations = duplicates.loc[
                duplicates["data_source"] == duplicate_data_source
            ]
            if mergeable_stations.empty:
                continue

            selected_station = mergeable_stations.iloc[0][current_station.index].copy()
            selected_station.loc[["is_duplicate", "merged_attributes"]] = False, True

            # TODO check what we do instead this line
            #self.stations_gdf.loc[current_station.name] = selected_station

            break
        return selected_station


    def run(self):
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

        # First get list of stations esp. their coordinates
        get_stations_list_sql = """
            SELECT id as station_id, coordinates FROM stations
        """
        if self.is_test:
            munich_center_coordinates = "POINT (11.4717 48.1548)"
            get_stations_list_sql = """
                SELECT id as station_id, coordinates FROM stations 
                WHERE ST_Dwithin(ST_PointFromWkb(coordinates, 4326)::geography, 
                              ST_PointFromText('{center_coordinates}', 4326)::geography, 
                              {radius_m}); 
            """.format(center_coordinates=munich_center_coordinates, radius_m=5000)

        gdf: gpd.GeoDataFrame = gpd.read_postgis(get_stations_list_sql,
                                                 con=self.con, geom_col="coordinates")
        # For each station's coordinate find all surrounding stations within a certain radius (including itself)
        radius_m = 100
        score_threshold: float = 0.49
        score_weights = dict(operator=0.2, address=0.1, distance=0.7)
        for idx in tqdm(range(gdf.shape[0])):
            current_station: pd.Series = gdf.iloc[idx]
            #print(f"{current_station}")

            duplicates, current_station_full = self.merge(current_station['station_id'], current_station['coordinates'], radius_m, score_threshold, score_weights)
            if not duplicates.empty:
                selected_station = self._merge_duplicates(current_station_full, duplicates)


    def merge(self, current_station_id, current_station_coordinates, radius_m, score_threshold, score_weights,
              filter_by_source_id: bool = False) -> (pd.DataFrame, pd.Series):

        find_surrounding_stations_sql = """ 
            SELECT 
                s.id as station_id,
                s.source_id as source_id,
                s.data_source, s.coordinates, s.operator,
                c.capacity,
                a.street, a.town,
                ST_DISTANCE(ST_PointFromWkb(s.coordinates, 4326)::geography, 
                              ST_PointFromText('{center_coordinates}', 4326)::geography) as distance
            FROM stations s
            LEFT JOIN charging c ON s.id = c.station_id
            LEFT JOIN address a ON s.id = a.station_id 
            WHERE ST_Dwithin(ST_PointFromWkb(s.coordinates, 4326)::geography, 
                              ST_PointFromText('{center_coordinates}', 4326)::geography, 
                              {radius_m});
        """

        sql = find_surrounding_stations_sql.format(station_id=current_station_id,
                                                   center_coordinates=current_station_coordinates,
                                                   radius_m=radius_m)
        #print(sql)
        nearby_stations: gpd.GeoDataFrame = gpd.read_postgis(sql, con=self.con, geom_col="coordinates")

        station_id_name = 'station_id'
        if filter_by_source_id:
            station_id_name = 'source_id'

        pd.set_option('display.max_columns', None)
        #print(f"All stations in radius: {nearby_stations}")
        #print(f"Current station: {nearby_stations[nearby_stations[station_id_name] == current_station_id]}")
        # skip if only center station itself was found
        if len(nearby_stations) >= 2:
            # get the current station with all it's attributes
            current_station_full: pd.Series = \
                nearby_stations[nearby_stations[station_id_name] == current_station_id].squeeze()

            if not current_station_full.empty:
                duplicates: pd.DataFrame = attribute_match_thresholds_strategy.attribute_match_thresholds_duplicates(
                    current_station=current_station_full,
                    duplicate_candidates=nearby_stations[nearby_stations[station_id_name] != current_station_id],
                    score_threshold=score_threshold,
                    max_distance=radius_m,
                    score_weights=score_weights,
                )
                return duplicates, current_station_full

        return pd.DataFrame(), pd.Series()





