import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd
import folium

from .html_helper import legend_html_bumpy_roads
from .useful_functs import filter_bike_data_location

import psycopg2


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


class BumpyRoads(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        self.data_base_dir = "/pygeoapi/data"
        self.html_out = "/pygeoapi/data/html"
        self.db_config = {
            "dbname": os.getenv("DATABASE_NAME"),
            "user": os.getenv("DATABASE_USER"),
            "password": os.getenv("DATABASE_PASSWORD"),
            "host": os.getenv("DATABASE_HOST"),
            "port": os.getenv("DATABASE_PORT"),
        }

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

        # if self.boxid not in os.listdir(self.data_base_dir):
        #     LOGGER.info(f'download data for {self.boxid}')
        #     OSM = OpenSenseMap.OpenSenseMap()
        #     OSM.add_box(self.boxid)
        #     OSM.save_OSM()

        # script
        atrai_bike_data = pd.read_csv("/pygeoapi/combined_data.csv")
        device_counts = atrai_bike_data.groupby("device_id").size()
        valid_device_ids = device_counts[device_counts >= 10].index
        atrai_bike_data = atrai_bike_data[
            atrai_bike_data["device_id"].isin(valid_device_ids)
        ]
        filtered_data_MS = filter_bike_data_location(atrai_bike_data)

        road_roughness = filtered_data_MS[
            [
                "Surface Asphalt",
                "Surface Sett",
                "Surface Compacted",
                "Surface Paving",
                "lng",
                "lat",
            ]
        ].copy()
        road_roughness = road_roughness.dropna(
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
        road_roughness_clean = road_roughness.dropna(
            subset=["lat", "lng", "Roughness_Normalized"]
        )

        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Delete all rows from the table
            cursor.execute("DELETE FROM road_roughness")

            insert_query = """
            INSERT INTO road_roughness (id, geom, roughness)
            VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            """

            for index, row in road_roughness_clean.iterrows():
                cursor.execute(
                    insert_query, (index, row['lng'], row['lat'], row["Roughness_Normalized"])
                )

            # Commit the transaction
            conn.commit()

            # Return a success message
            outputs = {
                "id": "bumpy_roads",
                "status": f"""Inserted {len(road_roughness_clean)} rows into the database""",
            }

        except Exception as e:
            # Rollback in case of error
            conn.rollback()
            outputs = {"id": "bumpy_roads", "status": f"""Error: {e}"""}

        finally:
            # Close the database connection
            cursor.close()
            conn.close()

        # m_roughness = folium.Map(location=[51.9607, 7.6261], zoom_start=14)

        # for idx, row in road_roughness_clean.iterrows():
        #     lat = row['lat']
        #     lng = row['lng']
        #     roughness = row['Roughness_Normalized']
        #     color = get_color(roughness)
        #     folium.CircleMarker([lat, lng], radius=5, color=color, fill=True, fill_color=color, fill_opacity=1).add_to(
        #         m_roughness)

        # legend = folium.Element(legend_html_bumpy_roads)
        # m_roughness.get_root().html.add_child(legend)

        # os.makedirs(self.html_out, exist_ok=True)
        # m_roughness.save(os.path.join(self.html_out, "road_roughness_colored_map.html"))

        return mimetype, outputs

    def __repr__(self):
        return f"<BumpyRoads> {self.name}"
