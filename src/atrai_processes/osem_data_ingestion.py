import os
import logging
from opensensemaptoolbox import OpenSenseMap
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from sqlalchemy import create_engine
import geopandas as gpd


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
        self.tag = data.get("tag") or "bike"

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

       
        OSM = OpenSenseMap.OpenSenseMap()
        OSM.box_sensor_dict_by_tag(self.tag)
        gdfs = OSM.get_gdfs()

        DB_URL = 'postgresql://%s:%s@%s:%s/%s' % (
            self.db_config['user'],
            self.db_config['password'],
            self.db_config['host'],
            self.db_config['port'],
            self.db_config['dbname']
        )
        engine = create_engine(DB_URL)

        for gdf in gdfs:
            gdf.to_postgis("osem_bike_data", engine, if_exists="append", index=False)


        outputs = {"id": "osem_data_ingestion", "status": f'''ingested {len(gdfs)} boxes'''}
        return mimetype, outputs

    def __repr__(self):
        return f"<OsemDataIngestion> {self.name}"
