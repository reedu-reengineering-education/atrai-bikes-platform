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
    "id": "Distances",
    "title": {
        "en": "Distances",
    },
    "description": {"en": "Distances from overtaking manoeuvres"},
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


class Distances(AtraiProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        self.db_config = DatabaseConfig()

    def execute(self, data):
        mimetype = "application/json"

        self.check_request_params(data)
        atrai_bike_data = self.load_data()

        # self.boxid = data.get("id")
        self.token = data.get("token")

        # if self.boxid is None:
        #     raise ProcessorExecuteError("Cannot process without an id")
        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with a valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        engine = self.db_config.get_engine()

        try:

            # Load road network
            road_segments = gpd.read_postgis(
                "SELECT * FROM bike_road_network",
                con=engine,
                geom_col="geometry"
            )
            if road_segments.crs is None:
                road_segments.set_crs(epsg=4326, inplace=True)  # Replace 4326 with the correct CRS if needed

            if road_segments.empty:
                raise ProcessorExecuteError("No road network data found")

            # Load raw sensor data
            # atrai_bike_data = gpd.read_postgis(
            #     "SELECT * FROM osem_bike_data",
            #     con=engine,
            #     geom_col="geometry"
            # )
            if atrai_bike_data.crs is None:
                atrai_bike_data.set_crs(epsg=4326, inplace=True)  # Replace 4326 with the correct CRS if needed

            # Reproject atrai_bike_data to match road_segments CRS
            if atrai_bike_data.crs != road_segments.crs:
                atrai_bike_data = atrai_bike_data.to_crs(road_segments.crs)

            # Filtering & preprocessing
            filtered_data = atrai_bike_data.copy()
            filtered_data["createdAt"] = pd.to_datetime(filtered_data["createdAt"])
            filtered_data = filtered_data.dropna(subset=["Overtaking Distance"])
            filtered_data = filtered_data[
                (filtered_data["Overtaking Manoeuvre"] > 0.05) &
                (filtered_data["Overtaking Distance"] > 0)
            ]
            filtered_data["Normalized Overtaking Distance"] = (
                filtered_data["Overtaking Distance"] / 200
            ).clip(upper=1)

            # Add id for grouping
            filtered_data["id"] = filtered_data.index

            # Map points to roads and aggregate
            overtaking_flowmap = map_points_to_road_segments(
                point_gdf=filtered_data,
                road_segments=road_segments,
                numeric_columns=[
                    "Overtaking Distance",
                    "Overtaking Manoeuvre",
                    "Normalized Overtaking Distance"
                ],
                id_column="id"
            )

            # Save to PostGIS
            overtaking_flowmap.to_postgis(
                "distances_flowmap",
                engine,
                if_exists="replace",
                index=False
            )

            outputs = {
                "id": "distances_flowmap",
                "status": f"Processed {len(overtaking_flowmap)} road segments with overtaking data"
            }

            return mimetype, outputs
        finally:
            engine.dispose()  # Ensure all connections are closed

    def __repr__(self):
        return f"<Distances> {self.name}"
