import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

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


class Distances(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        self.db_config = DatabaseConfig()

    def execute(self, data):
        mimetype = "application/json"

        self.boxid = data.get("id")
        self.token = data.get("token")

        if self.boxid is None:
            raise ProcessorExecuteError("Cannot process without a id")
        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        engine = self.db_config.get_engine()

        road_network_query = "SELECT * FROM bike_road_network"
        edges_filtered = gpd.read_postgis(
            road_network_query, engine, geom_col="geometry"
        )

        if len(edges_filtered) == 0:
            raise ProcessorExecuteError("No road network data found")

        atrai_bike_data = gpd.read_postgis(
            "SELECT * FROM osem_bike_data",
            con=engine,
            geom_col="geometry",
        )

        # filtered_data_MS = filter_bike_data_location(atrai_bike_data)
        filtered_data_MS = atrai_bike_data

        # filtered_data_MS = filtered_data_MS[['createdAt', 'lat', 'lng', 'device_id', 'Overtaking Distance', 'Overtaking Manoeuvre']]
        filtered_data_MS["createdAt"] = pd.to_datetime(filtered_data_MS["createdAt"])
        filtered_data_MS = filtered_data_MS.dropna(subset=["Overtaking Distance"])
        filtered_data_MS = filtered_data_MS[
            filtered_data_MS["Overtaking Manoeuvre"] > 0.05
        ]
        filtered_data_MS = filtered_data_MS[filtered_data_MS["Overtaking Distance"] > 0]
        filtered_data_MS["Normalized Overtaking Distance"] = (
            atrai_bike_data["Overtaking Distance"] / 200
        ).clip(upper=1)

        print(filtered_data_MS.head())

        edges_filtered = edges_filtered.reset_index(drop=True)

        # filtered_data_MS is a gdf. join each point to the nearest road segment and calculate the average distance for each road segment
        # Spatial join to find the nearest road segment for each point
        filtered_data_MS = filtered_data_MS.set_geometry("geometry")
        edges_filtered = edges_filtered.set_geometry("geometry")

        # Ensure both GeoDataFrames use the same CRS
        filtered_data_MS = filtered_data_MS.to_crs(edges_filtered.crs)

        # Perform a spatial join to find the nearest road segment
        filtered_data_with_roads = gpd.sjoin_nearest(
            filtered_data_MS,
            edges_filtered,
            how="left",
            distance_col="distance_to_road",
        )

        # print(filtered_data_with_roads.head())

        # Calculate the average "Overtaking Distance" and "Overtaking Manoeuvre" for each road segment
        distances_flowmap = filtered_data_with_roads.groupby("index_right").agg(
            {
                "Overtaking Distance": "mean",
                "Overtaking Manoeuvre": "mean",
                "Normalized Overtaking Distance": "mean",
                "distance_to_road": "mean",
                "createdAt": "count",
            }
        )

        # Rename the columns
        distances_flowmap = distances_flowmap.rename(
            columns={
                "Overtaking Distance": "Average Overtaking Distance",
                "Overtaking Manoeuvre": "Average Overtaking Manoeuvre",
                "Normalized Overtaking Distance": "Average Normalized Overtaking Distance",
                "distance_to_road": "Average Distance to Road",
                "createdAt": "Number of Points",
            }
        )

        # Reset the index
        # distances_flowmap = distances_flowmap.reset_index()

        # Add the geometry of the road segment
        distances_flowmap = distances_flowmap.merge(
            edges_filtered, left_on="index_right", right_index=True
        )

        # Drop all columns except the ones we need, also the id column
        distances_flowmap = distances_flowmap[
            [
                "Average Overtaking Distance",
                "Average Overtaking Manoeuvre",
                "Average Normalized Overtaking Distance",
                "Average Distance to Road",
                "Number of Points",
                "geometry",
            ]
        ]

        # Add the id column
        distances_flowmap["id"] = distances_flowmap.index

        # Ensure distances_flowmap is a GeoDataFrame with a geometry column
        distances_flowmap = gpd.GeoDataFrame(
            distances_flowmap, geometry="geometry", crs=edges_filtered.crs
        )

        # Save the results to the database
        distances_flowmap.to_postgis("distances_flowmap", engine, if_exists="replace")

        outputs = {
            "id": "distances_flowmap",
            "status": f"""Processed {len(distances_flowmap)} road segments""",
        }

        return mimetype, outputs

    def __repr__(self):
        return f"<Distances> {self.name}"
