import json

from charging_stations_pipelines import COUNTRIES

print(json.dumps(COUNTRIES, default=vars, indent=4))
