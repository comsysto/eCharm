from models.station import Station


def map_address(df):
    stations_df_mapped = df
    stations_df_mapped.rename(columns={'Betreiber': 'operator', 'Straße': 'street', 'Hausnummer': 'house_number'}, inplace=True)
    return stations_df_mapped


def create_station(row):
    new_station = Station()
    new_station.operator = row.loc['Betreiber']
    new_station.data_source = "DE"
    new_station.street = row.loc['Straße']
    new_station.house_number = row.loc['Hausnummer']
    #coordinates = Point(
    #    float(row['ChargeDeviceLocation.Longitude']),
    #    float(row['ChargeDeviceLocation.Latitude'])
    #).wkt
    #new_stations.coordinates = coordinates
    return new_station
