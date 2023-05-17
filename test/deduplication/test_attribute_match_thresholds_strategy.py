from unittest import TestCase

import pandas as pd

from charging_stations_pipelines.deduplication.attribute_match_thresholds_strategy import \
    attribute_match_thresholds_duplicates


class Test(TestCase):
    def test_attribute_match_thresholds_duplicates(self):
        station_to_check = pd.Series({
            "is_duplicate": True,
            "source_id": "6e361c5d2e01a72f11478ae6ce14ae3f9f293c85a7dcda",
            "data_source": "BNA",
            "operator": "EnBW mobility+ AG und Co.KG",
            "address": "Flurstück 313,Langenau/Seligweiler",
            "distance": 0.0
        })

        # identical should be marked as duplicate
        duplicate_candidate_1 = {
            "is_duplicate": False,
            "source_id": "juizgkjhgk8797",
            "data_source": "BNA",
            "operator": "EnBW mobility+ AG und Co.KG",
            "address": "Flurstück 313,Langenau/Seligweiler",
            "distance": 0.0
        }

        #same operator - should be marked as duplicate
        duplicate_candidate_2 = {
            "is_duplicate": False,
            "source_id": "9063198728",
            "data_source": "OSM",
            "operator": "EnBW mobility+ AG und Co.KG",
            "address": "xxx",
            "distance": 99.0
        }

        #same address - should be marked as duplicate
        duplicate_candidate_3 = {
            "is_duplicate": False,
            "source_id": "9063198707",
            "data_source": "OSM",
            "operator": "xxx",
            "address": "Flurstück 313,Langenau/Seligweiler",
            "distance": 99.0
        }

        # exceeds all threasholds, no duplicate
        duplicate_candidate_4 = {
            "is_duplicate": False,
            "source_id": "9063198709",
            "data_source": "OSM",
            "operator": "xxx",
            "address": "xxx",
            "distance": 90.0
        }

        duplicate_candidates = pd.DataFrame(
            [duplicate_candidate_1, duplicate_candidate_2, duplicate_candidate_3, duplicate_candidate_4])

        actual = attribute_match_thresholds_duplicates(station_to_check, duplicate_candidates, "unused")

        print(actual)
        print(actual.iloc[0])
