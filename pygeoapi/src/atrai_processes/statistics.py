import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd

# from .useful_functs import filter_bike_data_location

import psycopg2


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

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        # script
        atrai_bike_data = pd.read_csv("/pygeoapi/combined_data.csv")

        filtered_devices_by_tag = atrai_bike_data[atrai_bike_data["tag"] == self.tag]
        device_counts = filtered_devices_by_tag.groupby("device_id").size()


        # valid_device_ids = device_counts[device_counts >= 10].index
        # atrai_bike_data = atrai_bike_data[atrai_bike_data['device_id'].isin(valid_device_ids)]
        # filtered_data_MS = filter_bike_data_location(atrai_bike_data)

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Example: Insert data into a table
            insert_query = """
            INSERT INTO statistics (count)
            VALUES (%s)
            """

            cursor.execute("DROP TABLE IF EXISTS statistics")
            cursor.execute(
                "CREATE TABLE statistics (count INT)"
            )
            cursor.execute(
                insert_query, (len(device_counts),)
            )

            # Commit the transaction
            conn.commit()

            # Return a success message
            outputs = {
                "id": "statistics",
                "status": f"""Calculated statistics for {len(device_counts)} devices""",
            }

        except Exception as e:
            # Rollback in case of error
            conn.rollback()
            outputs = {"id": "statistics", "status": f"""Error: {e}"""}

        finally:
            # Close the database connection
            cursor.close()
            conn.close()

        return mimetype, outputs

    def __repr__(self):
        return f"<Statistics> {self.name}"
