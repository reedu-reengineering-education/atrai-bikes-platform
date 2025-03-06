import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import osmnx as ox
from sqlalchemy import create_engine

LOGGER = logging.getLogger(__name__)

METADATA = {
    "version": "0.2.0",
    "id": "road_network",
    "title": {
        "en": "road_network",
    },
    "description": {"en": "processes to calculate road network"},
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
    "inputs": {
        "token": {
            "title": "secret token",
            "description": "identify yourself",
            "schema": {"type": "string"},
        },
        "location": {
            "title": "location",
            "description": "location to get the data from",
            "schema": {"type": "string"},
        },
    },
    "outputs": {
        "id": {
            "title": "ID",
            "description": "The ID of the process execution",
            "schema": {"type": "string"},
        },
        "status": {
            "title": "status",
            "description": "describes process",
            "schema": {"type": "string"},
        },
    },
    "example": {"inputs": {"token": "ABC123XYZ666", "location": "Münster, Germany"}},
}


class RoadNetwork(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        self.data_base_dir = "/pygeoapi/data"
        self.db_config = {
            "dbname": os.getenv("DATABASE_NAME"),
            "user": os.getenv("DATABASE_USER"),
            "password": os.getenv("DATABASE_PASSWORD"),
            "host": os.getenv("DATABASE_HOST"),
            "port": os.getenv("DATABASE_PORT"),
        }

    def execute(self, data):
        mimetype = "application/json"

        self.token = data.get("token")
        self.location = data.get("location")

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        road_network = ox.graph_from_place(self.location, network_type="bike")
        _, edges = ox.graph_to_gdfs(road_network)
        edges_filtered = edges[~edges['highway'].isin(['primary', 'secondary', 'tertiary'])]
        DB_URL = 'postgresql://%s:%s@%s:%s/%s' % (
            self.db_config['user'],
            self.db_config['password'],
            self.db_config['host'],
            self.db_config['port'],
            self.db_config['dbname']
        )
        engine = create_engine(DB_URL)
        edges_filtered.to_postgis("bike_road_network", engine, if_exists="append", index=False)

        
        outputs = {
            "id": "road_network",
            "status": f"""road network data imported""",
        }

      
        return mimetype, outputs

    def __repr__(self):
        return f"<RoadNetwork> {self.name}"
