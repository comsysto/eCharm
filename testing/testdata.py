from dataclasses import dataclass
from typing import Optional, List
import os
import pathlib

from sqlalchemy import create_engine
from settings import db_uri
import pandas as pd

import testdata_import
from src.pipelines._merger import StationMerger

@dataclass
class TestData:
    osm_id: Optional[str] = None
    ocm_id: Optional[str] = None
    bna_id: Optional[str] = None
    osm_coordinates: Optional[str] = None


def load_test_data() -> List[TestData]:
    '''
    Create TestData objects from spreadsheet data
    '''
    rows = testdata_import.main()
    if not rows:
        print('No test data found.')
        return []
    test_data = []
    for row in rows[1:]:
        print(f"row: {row}")
        if not row or not row[0]:
            continue

        test_column = TestData()
        test_column.osm_id = row[10]
        ocm_id = row[13]
        if ocm_id and "OCM" in ocm_id:
            ocm_id = ocm_id.split("OCM-")[1:]
            test_column.ocm_id = ocm_id
        test_column.bna_id = None
        osm_latitude = row[11].replace(",", ".")
        osm_longitude = row[12].replace(",", ".")
        if not osm_latitude == "NO":
            test_column.osm_coordinates = f"POINT ({osm_longitude} {osm_latitude})"

        print(test_column)
        test_data.append(test_column)

    return test_data


def run():

    test_data = load_test_data()

    radius_m = 100
    score_threshold: float = 0.49
    score_weights = dict(operator=0.2, address=0.1, distance=0.7)

    import configparser

    config: configparser = configparser.RawConfigParser()
    current_dir = os.path.join(pathlib.Path(__file__).parent.resolve())
    config.read(os.path.join(os.path.join(current_dir, "config", "config.ini")))

    merger: StationMerger = StationMerger(config=config, con=create_engine(db_uri, echo=True))

    #print(test_data)
    with open("testdata_merge.csv", "w") as outfile:
        for station in test_data:
            duplicates: pd.DataFrame = pd.DataFrame()
            if station.osm_coordinates:
                #print(f"OSM ID of central charging station: {station.osm_id}")
                duplicates, current_station = merger.find_duplicates(station.osm_id, station.osm_coordinates, radius_m, score_threshold, score_weights, filter_by_source_id=True)
            if not duplicates.empty:
                data_sources_in_duplicates = ','.join(duplicates.data_source.unique())
                #print(f"Data Sources in duplicates: {data_sources_in_duplicates}")
                result = [station.osm_id, str(len(duplicates)), data_sources_in_duplicates]
                print(f"Result of merge on test data: {result}")
                outfile.write(','.join(result))
                outfile.write('\n')
            else:
                print(f"No successful merge on test data")
                outfile.write('NA\n')

            if station.osm_id == '6417375309':
                pass#break
        outfile.close()

if __name__ == '__main__':
    run()
