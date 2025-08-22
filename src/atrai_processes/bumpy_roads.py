import os
import logging
from .map_points_to_road_network import map_points_to_road_segments
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .atrai_processor import AtraiProcessor

import pandas as pd

import geopandas as gpd

from config.db_config import DatabaseConfig


LOGGER = logging.getLogger(__name__)

METADATA = {
    "version": "0.2.0",
    "id": "bumpy_roads",
    "title": {
        "en": "bumpy_roads",
    },
    "description": {"en": "evaluates bumpiness of roads"},
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
        "id": {
            "title": "boxid",
            "description": "boxid to get the data from",
            "schema": {"type": "string"},
        },
        "token": {
            "title": "secret token",
            "description": "identify yourself",
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
    "example": {"inputs": {"ids": "ABCDEF123456", "token": "ABC123XYZ666"}},
}


# Color based on the roughness index
def get_color(roughness):
    if roughness <= 20:
        return "green"
    elif roughness <= 40:
        return "lightgreen"
    elif roughness <= 60:
        return "yellow"
    elif roughness <= 80:
        return "orange"
    else:
        return "red"


# Function to calculate roughness score
def calculate_roughness(
    row, roughness_scores=dict(Asphalt=1, Paving=2, Compacted=3, Sett=4)
):
    score = 0
    score += roughness_scores["Asphalt"] * (row["Surface Asphalt"])
    score += roughness_scores["Paving"] * (row["Surface Paving"])
    score += roughness_scores["Compacted"] * (row["Surface Compacted"])
    score += roughness_scores["Sett"] * (row["Surface Sett"])
    return score


class BumpyRoads(AtraiProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        # self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        # self.data_base_dir = "/pygeoapi/data"
        # self.html_out = "/pygeoapi/data/html"
        self.db_config = DatabaseConfig()

    def execute(self, data):
        mimetype = "application/json"

        self.check_request_params(data)
        atrai_bike_data = self.load_data()

        # self.boxid = data.get("id")
        self.token = data.get("token")

        # if self.boxid is None:
        #     raise ProcessorExecuteError("Cannot process without a id")
        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        engine = self.db_config.get_engine()

        road_network_query = "SELECT * FROM bike_road_network"
        road_segments = gpd.read_postgis(road_network_query, engine, geom_col="geometry")
        if len(road_segments) == 0:
            raise ProcessorExecuteError("No road network data found")

        # Step 2: Load raw bike data
        # atrai_bike_data = gpd.read_postgis(
        #     "SELECT * FROM osem_bike_data",
        #     con=engine,
        #     geom_col="geometry",
        # )

        road_roughness = atrai_bike_data.dropna(
            subset=[
                "Surface Asphalt",
                "Surface Sett",
                "Surface Compacted",
                "Surface Paving",
            ]
        )

        road_roughness["Roughness"] = road_roughness.apply(calculate_roughness, axis=1)
        road_roughness["Roughness_Normalized"] = (
            road_roughness["Roughness"] / road_roughness["Roughness"].max()
        ) * 100
        road_roughness_clean = road_roughness.dropna(subset=["Roughness_Normalized"])

        road_roughness_clean["id"] = road_roughness_clean.index
        road_roughness_clean = road_roughness_clean.drop(
            columns=[
                "Temperature",
                "Rel. Humidity",
                "Finedust PM1",
                "Finedust PM2.5",
                "Finedust PM4",
                "Finedust PM10",
                "Overtaking Manoeuvre",
                "Overtaking Distance",
                "Surface Anomaly",
                "Speed",
            ],
            errors="ignore"  # if any column is missing
        )

       # Map to road network and aggregate
        roughness_flowmap = map_points_to_road_segments(
            point_gdf=road_roughness_clean,
            road_segments=road_segments,
            numeric_columns=["Roughness", "Roughness_Normalized"],
            id_column="id"
        )

        roughness_flowmap.to_postgis(
            "road_roughness",
            engine,
            if_exists="replace",
            index=False
        )

        outputs = {
            "id": "road_roughness",
            "status": f"Processed {len(roughness_flowmap)} road segments with roughness data"
        }

        return mimetype, outputs

    def __repr__(self):
        return f"<BumpyRoads> {self.name}"
