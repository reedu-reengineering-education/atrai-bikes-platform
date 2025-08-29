import os
import requests
import logging

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)


PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "data_ingestion",
    "title": {
        "en": "data_ingestion",
    },
    "description": {"en": "ingesting all ds"},
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["process"],
    "links": [
        {
            "type": "text/html",
            "rel": "about",
            "title": "information",
            "href": "https://example.org/process",
            "hreflang": "en-US",
        }
    ],
    "inputs": {"input": {"title": "result", "description": "The URL of the result", "schema": {"type": "string"}}},
    "outputs": {
        "id": {"title": "ID", "description": "The ID of the process execution", "schema": {"type": "string"}},
        "value": {
            "title": "Value",
            "description": "The URL of the Zarr file in the S3 bucket",
            "schema": {"type": "string"},
        },
    },
    "example": {"inputs": {"result_json": "www.j.son"}},
}


class DataIngestion(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN")
        self.api_url_base = os.environ.get("API_URL", 'http://localhost:80')

        self.ingestion_dict = {
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
        self.processes = [
            'road_network',
            'distances',
            'statistics',
            'bumpy-roads',
            'dangerous-places',
            'speed-traffic-flow'
        ]
        self.campaigns = [
            'arnsberg',
            'greifswald',
            'saopaulo',
            'muenster',
            'wiesbaden'
        ]

        self.token = None
        self.input_campaigns = None
        self.input_processes = None


    def execute(self, data):
        mimetype = "application/json"

        self.token = data.get("token")
        self.input_campaigns = data.get("campaigns")
        self.input_processes = data.get("processes")

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        if self.input_processes == "all":
            processes = self.processes
        else:
            if isinstance(self.input_processes, list) and set(self.input_processes).issubset(set(self.processes)) and len(self.input_processes) > 0:
                if 'osem_data_ingestion' in self.input_processes:
                    raise ProcessorExecuteError(f"'osem_data_ingestion' not allowed in list")
                else:
                    processes = self.input_processes
            else:
                raise ProcessorExecuteError(f"processes is not a list or 'all'")

        if self.input_campaigns == "all":
            campaigns = self.campaigns
        else:
            if isinstance(self.input_campaigns, list) and set(self.input_campaigns).issubset(set(self.campaigns)) and len(self.input_campaigns) > 0:
                campaigns = self.input_campaigns
            else:
                raise ProcessorExecuteError(f"campaigns is not a list or 'all'")

        LOGGER.debug(f"starting with osem_data_ingestion")
        endpoint = os.path.join(self.api_url_base, f"processes/osem_data_ingestion/execution?f=json")
        payload = {
            "inputs": {
                "token": self.token
            }
        }
        try:
            requests.post(endpoint, json=payload).raise_for_status()
            LOGGER.debug("osem_data_ingestion successful")
        except requests.exceptions.RequestException as e:
            LOGGER.debug(f"Error: {e}")


        for campaign in campaigns:
            for process in processes:
                endpoint = os.path.join(self.api_url_base, f"processes/{process}/execution?f=json")
                if process == "road_network":
                    payload = {
                        "inputs": {
                            "campaign": campaign,
                            "token": self.token,
                            "location": self.ingestion_dict["road_network"][campaign]
                        }
                    }

                else:
                    payload = {
                        "inputs": {
                            "campaign": campaign,
                            "token": self.token,
                            "col_create": True
                        }
                    }
                LOGGER.debug(f"campaign: '{campaign}', process: '{process}'")
                try:
                    requests.post(endpoint, json=payload).raise_for_status()
                    LOGGER.debug(f"ingestions on '{endpoint}' for '{campaign}' successful")
                except requests.exceptions.RequestException as e:
                    LOGGER.debug(f"Error: {e}")

        outputs = {
            "id": "ingestion process",
            "status": f"""data ingested for '{self.input_campaigns}' campaigns for '{self.input_processes}' processes """,
        }

        return mimetype, outputs

    def __repr__(self):
        return f"<DataIngestion> {self.name}"




