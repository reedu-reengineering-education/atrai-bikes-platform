import os
import logging
from opensensemaptoolbox import OpenSenseMap
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from sqlalchemy import text

from config.db_config import DatabaseConfig



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

       
        OSM = OpenSenseMap.OpenSenseMap()
        OSM.box_sensor_dict_by_tag(self.tag)
        gdfs = OSM.get_gdfs()

        engine = self.db_config.get_engine()

        # delete existing data
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM osem_bike_data"))

        for gdf in gdfs:
            # get all columns in gdf
            columns = gdf.columns.tolist()
            # get all columns that are not in the table "osem_bike_data"
            with engine.begin() as conn:
                existing_columns = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='osem_bike_data'")).fetchall()
                existing_columns = [col[0] for col in existing_columns]
                new_columns = [col for col in columns if col not in existing_columns]

                print(f"existing columns: {existing_columns}")
                print(f"new columns: {new_columns}")

                # create new columns
                for col in new_columns:
                    conn.execute(text(f'ALTER TABLE osem_bike_data ADD COLUMN "{col}" DOUBLE PRECISION'))

            # set crs
            gdf = gdf.set_crs('epsg:4326', allow_override=True)
            # insert data
            gdf.to_postgis("osem_bike_data", engine, if_exists="append", index=False)

        # create id column and set as primary key 
        with engine.begin() as conn:
            # remove existing id column
            conn.execute(text("ALTER TABLE osem_bike_data DROP COLUMN IF EXISTS id"))
            # add new id column
            conn.execute(text("ALTER TABLE osem_bike_data ADD COLUMN id SERIAL PRIMARY KEY"))

        outputs = {"id": "osem_data_ingestion", "status": f'''ingested {len(gdfs)} boxes'''}
        return mimetype, outputs

    def __repr__(self):
        return f"<OsemDataIngestion> {self.name}"
