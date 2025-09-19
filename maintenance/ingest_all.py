import os
import requests
import time
import json
from datetime import datetime


local = True
ingestion = True

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
filename = f"ingestion_results_{timestamp}.json"
token = "token"
if local:
    path_base = './'
    api_url_base = "http://localhost:5000"
else:
    path_base = "/home/ubuntu/workspace/api/ingestion_logs"
    api_url_base = "http://api.atrai.bike"

file_path = os.path.join(path_base, filename)

ingestion_dict = {
    "road_network": {
        "arnsberg":
            [{"city": "Arnsberg", "county": "Hochsauerlandkreis", "country": "Germany"}],
        "greifswald":
            [{"city": "Greifswald", "country": "Germany"}],
        "saopaulo":
            [{"city": "Sao Paulo", "country": "Brazil"}],
        "muenster":
            [{"city": "Münster", "country": "Germany"}],
        "wiesbaden":
            [{"city": "Wiesbaden", "country": "Germany"},
             {"city": "Mainz", "country": "Germany"}],
    }
}


# 'osem_data_ingestion' needs to run anyways always
processes = [
    'road_network',
    'distances',
    'statistics',
    'bumpy-roads',
    'dangerous-places',
    'speed-traffic-flow'
]
campaigns = [
    'arnsberg',
    'greifswald',
    'saopaulo',
    'muenster',
    'wiesbaden'
]

d = dict()

if ingestion:
    print(f"starting with osem_data_ingestion")
    endpoint = os.path.join(api_url_base, f"processes/osem_data_ingestion/execution?f=json")
    payload = {
        "inputs": {
            "token": token
        }
    }
    try:
        res = requests.post(endpoint, json=payload)
        print("osem_data_ingestion successful")
        d['osem_data_ingestion'] = res.status_code
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")


for campaign in campaigns:
    for process in processes:
        endpoint = os.path.join(api_url_base, f"processes/{process}/execution?f=json")
        if process == "road_network":
            payload = {
                "inputs": {
                    "campaign": campaign,
                    "token": token,
                    "location": ingestion_dict["road_network"][campaign],
                    "col_create": True
                }
            }

        else:
            payload = {
                "inputs": {
                    "campaign": campaign,
                    "token": token,
                    "col_create": True
                }
            }
        print(f"campaign: '{campaign}', process: '{process}'")
        try:
            res = requests.post(endpoint, json=payload)
            if campaign not in d:
                d[campaign] = {}
            d[campaign][process] = res.status_code

            time.sleep(2)
            print(f"ingestions on '{endpoint}' for '{campaign}' successful")
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")




with open(file_path, 'w') as f:
    json.dump(d, f, indent=4)

print(d)
print(f"Ingestion results saved to {filename}")
