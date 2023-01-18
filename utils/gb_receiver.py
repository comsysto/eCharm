import json

import requests
from requests import Response


#http://chargepoints.dft.gov.uk/api/retrieve/{option1}/{value1}/{option2}/{value2}/
#https://chargepoints.dft.gov.uk/api/help
#option = format of data received: format[xml|json|csv] - Output format, default is 'xml'

def get_gb_data(tmp_data_path):

    api_url = "http://chargepoints.dft.gov.uk/api/retrieve/registry/format/json/"
    response: Response = requests.get(api_url)
    response.json()

    status_code: int = response.status_code
    if status_code != 200:
        raise RuntimeError(f"Failed to get GB-Gov-Data! Status-Code: {status_code}")   
    with open(tmp_data_path, "w") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=4, sort_keys=True)
  

if __name__ == "__main__":
    get_gb_data(tmp_data_path="./gb_gov_data.json")