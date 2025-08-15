import os
import sys 

# Get the directory of the current script for testing
script_dir = os.path.dirname(__file__)

# Go up one level from atrai_processes to src
src_path = os.path.abspath(script_dir) 
project_root = os.path.abspath(os.path.join(script_dir, '..', '..')) # Goes up from atrai_processes -> src -> project_root
src_dir_path = os.path.join(project_root, 'src') # Points to 'project_root/src'

# Add the 'src' directory to sys.path if it's not already there
if src_dir_path not in sys.path:
    sys.path.insert(0, src_dir_path)


import logging
from config.db_config import DatabaseConfig
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd

import geopandas as gpd

from sqlalchemy import text

# from .statistic_utils import process_tours, tour_stats
from atrai_processes.statistic_utils import process_tours, tour_stats



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

        try:
            # Step 1: Load raw bike data
            query = text("SELECT * FROM osem_bike_data WHERE grouptag LIKE :tag")
            atrai_bike_data = gpd.read_postgis(
                query,
                params={"tag": f"%{self.tag}%"},
                con=engine,
                geom_col="geometry",
            )
            
            if len(atrai_bike_data) == 0:
                raise ProcessorExecuteError("No data found for the given tag")
            
            # Step 2: Process tours
            tours = process_tours(atrai_bike_data, interval=12)
            stats = tour_stats(tours)

            # Step 3: Calculate convex hull 
            convex_hull = atrai_bike_data.unary_union.convex_hull

            # Step 4: Create GeoDataFrame containing one row with the bounding box and all the statistics
            bbox_gdf = gpd.GeoDataFrame(
                {
                    "tag": [self.tag],
                    "statistics": [stats],
                    # add stats to the GeoDataFrame, like with the spread operator in js
                    # **{f"{k}": [v] for k, v in stats.items()},
                    "updatedAt": [pd.Timestamp.now()],
                },
                geometry=[convex_hull],
                crs="EPSG:4326",
            )
        
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
                        dtype={"geometry": "geometry(Polygon, 4326)", "tag": "TEXT"}, # Explicity define tag as TEXT
                    )
                else:
                    # Upsert the data: delete the existing row with the same tag and insert the new one
                    conn.execute(
                        text("DELETE FROM statistics WHERE tag = :tag"), # reference the "tag" column which is created, not grouptag
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
        finally:
            engine.dispose()  # Ensure all connections are closed

    def __repr__(self):
        return f"<Statistics> {self.name}"
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    LOGGER.setLevel(logging.INFO) # Set logger level to INFO for detailed output

    print(" Starting test of Atrai Statistics Process ")

    # Create an instance of the processor
    processor = Statistics(processor_def={"name": "statistics"})

    # Define test input data
    test_data = {
        "token": os.environ.get("INT_API_TOKEN"),
        "tag": "muenster" # use tag muenster for test bc data exists
    }

    if test_data["token"] is None:
        LOGGER.error("INT_API_TOKEN environment variable not set. Please set it before running.")
        exit(1)

    try:
        mimetype, outputs = processor.execute(test_data)
        LOGGER.info(f"Process execution successful!")
        LOGGER.info(f"Mimetype: {mimetype}")
        LOGGER.info(f"Outputs: {outputs}")
        print("\n Test finished! ")
    except ProcessorExecuteError as e:
        LOGGER.error(f"Processor execution error: {e}")
        print("\n--- Direct test FAILED ---")
    except Exception as e:
        LOGGER.error(f"An unexpected error occurred during execution: {e}", exc_info=True)
        print("\n Test FAILED ")
