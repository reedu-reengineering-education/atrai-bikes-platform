import os
import logging
from opensensemaptoolbox import OpenSenseMap
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from sqlalchemy import text

from config.db_config import DatabaseConfig

import datetime as dt



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


class OsemDataIngestion(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        self.data_base_dir = "/pygeoapi/data"
        self.db_config = DatabaseConfig()

    def execute(self, data):
        mimetype = "application/json"

        self.token = data.get("token")
        self.tag = data.get("tag") or "bike"

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

       
        engine = self.db_config.get_engine()

        # Get the latest date from the database
        with engine.begin() as conn:
            latest_date = '1970-01-01T00:00:00Z'
            try:
                result = conn.execute(text('SELECT MAX("osem_bike_data"."createdAt") FROM "osem_bike_data" WHERE grouptag ...')).fetchone()
                # format the date to be compatible with the OpenSenseMap API
                latest_date = result[0].astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception as e:
                LOGGER.error(f"Error getting latest date from database: {e}")


        # Query data
        OSM = OpenSenseMap.OpenSenseMap()
        today = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        OSM.box_sensor_dict_by_tag(self.tag, from_date=latest_date, to_date=today)
        gdfs = OSM.get_gdfs()

        # Delete existing data
        # self._clear_existing_data(engine)

        # Process and insert data
        self._process_and_insert_data(engine, gdfs)

        outputs = {"id": "osem_data_ingestion", "status": f'''ingested {len(gdfs)} boxes'''}
        return mimetype, outputs
    
    # def _clear_existing_data(self, engine):
    #     with engine.begin() as conn:
    #         conn.execute(text("DELETE FROM osem_bike_data"))
    #         conn.execute(text("ALTER TABLE osem_bike_data ALTER COLUMN geometry TYPE geometry(Point, 4326) USING ST_SetSRID(geometry, 4326)"))

    def _process_and_insert_data(self, engine, gdfs):
        for gdf in gdfs:
            self._add_missing_columns(engine, gdf)
            gdf = gdf.set_crs('epsg:4326', allow_override=True)
            gdf.to_postgis("osem_bike_data", engine, if_exists="append", index=False)

        # Create id column and set as primary key
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE osem_bike_data DROP COLUMN IF EXISTS id"))
            conn.execute(text("ALTER TABLE osem_bike_data ADD COLUMN id SERIAL PRIMARY KEY"))

    def _add_missing_columns(self, engine, gdf):
        columns = gdf.columns.tolist()
        with engine.begin() as conn:
            existing_columns = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='osem_bike_data'")).fetchall()
            existing_columns = [col[0] for col in existing_columns]
            new_columns = [col for col in columns if col not in existing_columns]

            for col in new_columns:
                conn.execute(text(f'ALTER TABLE osem_bike_data ADD COLUMN "{col}" DOUBLE PRECISION'))

    def __repr__(self):
        return f"<OsemDataIngestion> {self.name}"
