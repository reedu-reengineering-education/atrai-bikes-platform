import os
import logging
from config.db_config import DatabaseConfig
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd

import geopandas as gpd

from sqlalchemy import text

from .statistic_utils import process_tours, tour_stats


from shapely.geometry import box








LOGGER = logging.getLogger(__name__)

METADATA = {
    "version": "0.2.0",
    "id": "statistics",
    "title": {
        "en": "statistics",
    },
    "description": {"en": "processes to calculate statistics"},
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
        "tag": {
            "title": "tag",
            "description": "tag to filter data",
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
    "example": {"inputs": {"token": "ABC123XYZ666", "tag": "tag"}},
}


class Statistics(BaseProcessor):
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
        self.tag = data.get("tag")
        self.db_config = DatabaseConfig()

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")
        
        engine = self.db_config.get_engine()

        if self.tag is None:
            raise ProcessorExecuteError("Cannot process without a tag")

        # Step 1: Load raw bike data
        query = text("SELECT * FROM osem_bike_data WHERE tags LIKE :tag")
        atrai_bike_data = gpd.read_postgis(
            query,
            params={"tag": f"%{self.tag}%"},
            con=engine,
            geom_col="geometry",
        )
        
        if len(atrai_bike_data) == 0:
            raise ProcessorExecuteError("No data found for the given tag")
        
        # Step 2: Process tours
        tours = process_tours(atrai_bike_data, intervall=15)
        stats = tour_stats(tours)

         # Step 3: Calculate bbox for the filtered data
        bbox = atrai_bike_data.total_bounds
        LOGGER.info(f"Bounding box for tag '{self.tag}': {bbox}")

        # Step 4: Create GeoDataFrame containing one row with the bounding box and all the statistics
        bbox_gdf = gpd.GeoDataFrame(
            {
                "tag": [self.tag],
                "statistics": [stats],
                # add stats to the GeoDataFrame, like with the spread operator in js
                # **{f"{k}": [v] for k, v in stats.items()},
                "updatedAt": [pd.Timestamp.now()],
            },
            geometry=[box(*bbox)],
            crs="EPSG:4326",
        )

        LOGGER.info(bbox_gdf.head())
    
        # Step 5: Upsert this data into the database, overwrite if exists (by tag). the tag is the primary key
        with engine.begin() as conn:
            # Check if the table exists
            if not engine.dialect.has_table(conn, "statistics"):
                # Create the table if it doesn't exist
                bbox_gdf.to_postgis(
                    name="statistics",
                    con=conn,
                    if_exists="replace",
                    index=False,
                    dtype={"geometry": "geometry(Polygon, 4326)"},
                )
            else:
                # Upsert the data: delete the existing row with the same tag and insert the new one
                conn.execute(
                    text("DELETE FROM statistics WHERE tag = :tag"),
                    {"tag": self.tag},
                )
                bbox_gdf.to_postgis(
                    name="statistics",
                    con=conn,
                    if_exists="append",
                    index=False,
                    dtype={"geometry": "geometry(Polygon, 4326)"},
                )


        outputs = {
            "id": self.tag,
            "status": "success",
            "message": f"Calculated statistics for tag '{self.tag}'",
            "statistics": stats
        }

        return mimetype, outputs

    def __repr__(self):
        return f"<Statistics> {self.name}"
