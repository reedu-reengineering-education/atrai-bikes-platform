import logging
import os

import requests
import yaml
from pygeoapi.process.base import BaseProcessor

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "danger levels creation",
    "title": {
        "en": "dangerlvls",
    },
    "description": {"en": "process that checks for health of collections"},
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


class CollectionHealthcheck(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.base_url = os.environ.get("API_URL", 'localhost:5000')
        self.col_base_url = os.path.join(self.base_url, 'collections')
        self.serv_config = os.environ.get(key="PYGEOAPI_SERV_CONFIG")
        self.input = None

    def execute(self, data):
        mimetype = "application/json"
        self.input = data.get("input")

        with open(self.serv_config, "r") as f:
            data = yaml.safe_load(f)

        faulty = []

        for ds in data["resources"]:
            url = os.path.join(self.col_base_url, ds)
            res = requests.get(url)
            print(ds, res.status_code)
            if res.status_code != 200:
                faulty.append({ds: res.status_code})

        outputs = {"id": "healthcheck faulty colls:", "value": faulty}
        LOGGER.debug("return")
        return mimetype, outputs

    def __repr__(self):
        return f"<CollectionHealthcheck> {self.name}"