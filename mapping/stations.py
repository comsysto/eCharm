from shapely.geometry import Point

from models.station import Station


def map_address(df):
    stations_df_mapped = df
    stations_df_mapped.rename(columns={'Betreiber': 'operator', 'Straße': 'street', 'Hausnummer': 'house_number'}, inplace=True)
    return stations_df_mapped


def map_stations_bna(row):
    new_station = Station()
    new_station.operator = row['Betreiber']
    new_station.data_source = "BNA"
    coordinates = Point(
        float(row['Längengrad']),
        float(row['Breitengrad'])
    ).wkt
    new_station.coordinates = coordinates
    new_station.date_created = row["Inbetriebnahmedatum"].strftime("%Y-%m-%d"),
    return new_station
