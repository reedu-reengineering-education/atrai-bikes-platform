import os
import logging
import opensensemaptoolbox as osmtb
import pandas as pd
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import yaml
from filelock import Timeout, FileLock



from sqlalchemy import text, inspect

from config.db_config import DatabaseConfig

import datetime as dt


LOGGER = logging.getLogger(__name__)

METADATA = {
    "version": "0.2.0",
    "id": "osem data ingestion",
    "title": {
        "en": "osem data ingestion",
    },
    "description": {"en": "ingest all osem datat per campaign"},
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
        self.db_cfg = self.db_config.get_db_config()
        self.config_file = os.environ.get('PYGEOAPI_SERV_CONFIG', '/pygeoapi/local.config.yml')
        self.tag = None
        self.boxes_metadata = pd.read_csv('/pygeoapi/src/boxes/metatable.csv')

    def read_config(self):
        with open(self.config_file, 'r') as file:
            LOGGER.debug("read config")
            return(yaml.safe_load(file))

    def write_config(self, new_config):
        with  open(self.config_file, 'w') as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")


    # def update_config(self):
    #     # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
    #     lock = FileLock(f"{self.config_file}.lock")

    #     with lock:

    #         config= self.read_config()
    #         config['resources'][f'merged_by_tag_{self.tag}'] = {
    #             'type': 'collection',
    #             'title': f'merged_by_tag_{self.tag}',
    #             'description': f"data of tag: '{self.tag}'",
    #             'keywords': ['country'],
    #             'extents': {
    #                 'spatial': {
    #                     'bbox': [-180, -90, 180, 90],
    #                     'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326'
    #                 },
    #             },
    #             'providers':
    #                 [{
    #                     'type': 'feature',
    #                     'name': 'PostgreSQL',
    #                     'data': {
    #                         'host': self.db_cfg['host'],
    #                         'port': self.db_cfg['port'],
    #                         'dbname': self.db_cfg['dbname'],
    #                         'user': self.db_cfg['user'],
    #                         'password': self.db_cfg['password'],
    #                         'search_path': ['public']
    #                     },
    #                     'id_field': 'index',
    #                     'table': f"""merged_by_tag_{self.tag}""",
    #                     'geom_field': 'geometry'
    #                 }]
    #         }

    #         self.write_config(config)


    def execute(self, data):
        mimetype = "application/json"

        self.token = data.get("token")

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        engine = self.db_config.get_engine()
        try:
            inspector = inspect(engine)
            existing_tables = inspector.get_table_names(schme="public") # TODO make an env var

            OSM = osmtb.OpenSenseMap()
            boxIds = [i for i in self.boxes_metadata['id']]
            OSM.add_box(boxIds)
            OSM.update_OSM(mode='postgis', engine=engine)

            OSM.fetch_box_data()

            OSM.merge_OSM()
            OSM.save_OSM(mode='postgis', engine=engine)
            OSM.merged_gdf.to_postgis(f"""osem_bike_data""", engine, if_exists="replace", index=True)

            msg = {'state' : 'OK',
                'message': f"ingested all data, Count of boxes: {len(boxIds)}"}
            # self.update_config()

            return mimetype, msg
        finally:
            engine.dispose()  # Ensure all connections are closed


    def __repr__(self):
        return f"<OsemDataIngestion> {self.name}"
