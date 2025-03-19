import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import osmnx as ox
from sqlalchemy import text


from config.db_config import DatabaseConfig


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
        self.db_config = DatabaseConfig()

    def execute(self, data):
        mimetype = "application/json"

        self.token = data.get("token")
        self.location = data.get("location")

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")
        
        filters = [
            '["highway"~"cycleway"]',
            '["highway"~"path"]["bicycle"~"designated|yes"]',
            '["highway"~"tertiary|residential|unclassified"]["bicycle"!~"use_sidepath"]["cycleway:both"!~"separate"]',
            '["highway"~"secondary|service"]["bicycle"~"yes"]',
            '["highway"~"secondary|service"]["cycleway"~"no|lane|track|opposite_lane|opposite_track|shared_lane|shared_track|share_busway"]',
            '["highway"~"secondary|service"]["cycleway:both"~"no|lane"]',
            '["bicycle_road"~"yes"]',
        ]

        # no footway, no highway primary
        road_network = ox.graph_from_place(
            self.location,
            custom_filter=filters,
            # network_type="bike",
            # simplify=True,
        )
        _, edges = ox.graph_to_gdfs(road_network)

        engine = self.db_config.get_engine()

        edges.to_postgis("bike_road_network", engine, if_exists="append", index=False)

        outputs = {
            "id": "road_network",
            "status": f"""road network data imported. {edges.shape[0]} edges""",
        }

        return mimetype, outputs

    def __repr__(self):
        return f"<RoadNetwork> {self.name}"
