import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from sqlalchemy import create_engine, text
import pandas as pd
import geopandas as gpd
import datetime
import yaml
from filelock import FileLock

LOGGER = logging.getLogger(__name__)


class AtraiProcessor(BaseProcessor):
    def __init__(self, processor_def, METADATA):
        super().__init__(processor_def, METADATA)

        self.secret_token = os.environ.get('INT_API_TOKEN', 'token')
        self.data_base_dir = os.environ.get('BASE_DATA_DIR')
        self.html_out = os.environ.get('HTML_OUT_DIR')
        self.config_file = os.environ.get('PYGEOAPI_CONFIG')
        self.metatable_path = os.environ.get('META_TABLE_PATH')
        self.db_host = os.environ.get("DATABASE_HOST", "localhost")
        self.db_port = os.environ.get("DATABASE_PORT", "5432")
        self.db_name = os.environ.get("DATABASE_NAME", "geoapi_db")
        self.db_user = os.environ.get("DATABASE_USER", "postgres")
        self.db_password = os.environ.get("DATABASE_PASSWORD", "postgres")
        self.db_engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

        self.campaign = None
        self.t_start = None
        self.t_end = None
        self.boxId = None
        self.col_create = None
        self.token = None
        self.metatable = pd.read_csv(self.metatable_path)

        self.id_field = 'id'
        self.mimetype = "application/json"
        self.data = None

    def check_request_params(self, data):
        # example data
        # its either capaign or boxId to filter the dataset by
        # boixId could also be a list of ids
        # if both capaign & boxId are selected boxId will be used and col_create will be false
        # {
        #     "capaign": "ms",
        #     "boxId": ['uu11ii22dd33'],
        #     "t_start": "2025-05-12T10:59:00",
        #     "t_end": "2025-08-12T12:00:00",
        #     "col_create": True,
        #     "token": "123456"
        #
        # }

        self.campaign = data.get('campaign')
        self.boxId = data.get('boxId')
        self.t_start = data.get('t_start')
        self.t_end = data.get('t_end')
        self.col_create = data.get('col_create')
        self.token = data.get('token')

        self.title = None


        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        if not self.boxId and not self.campaign:
            msg = "neither campaign nor boxid provided - cannot continue"
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        if self.boxId and not isinstance(self.boxId, list):
            msg = "boxId needs to be a list of boxIds - can be a single one e.g. ['123456abcdef']"
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        if self.campaign and self.campaign not in self.metatable['location'].values:
            msg = f""" '{self.campaign}' is not in metatable. Valid values are '{", ".join([c for c in self.metatable['location'].unique()])}' """
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        if self.boxId and not all(i in self.metatable['id'].values for i in self.boxId):
            msg = f""" ""'{self.boxId}' is not in metatable. Valid values are {", ".join([c for c in self.metatable['id'].unique()])} """
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        if self.t_start and self.t_end:
            if datetime.datetime.fromisoformat(self.t_start) >= datetime.datetime.fromisoformat(self.t_end):
                msg = f"t_start: '{self.t_start}' is bigger than t_end: '{self.t_end}'"
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

        if self.campaign is not None and self.boxId is not None:
            self.boxId = None
            self.col_create = False


    def load_bike_data(self):
        sql_base = "SELECT * FROM osem_bike_data"
        filters = []
        params = {}

        if self.campaign:
            ids = self.metatable['id'][self.metatable['location'] == self.campaign]
            self.boxId = [id for id in ids]

        if self.boxId:
            filters.append(""" "boxId" = ANY(:box_ids)""")
            params["box_ids"] = self.boxId

        if self.t_start and self.t_end:
            filters.append(""" "createdAt" BETWEEN :t_start AND :t_end""")
            params["t_start"] = self.t_start
            params["t_end"] = self.t_end

        # combine filters
        if filters:
            sql_base += " WHERE " + " AND ".join(filters)

        sql = text(sql_base)
        gdf = gpd.read_postgis(sql, self.db_engine, geom_col='geometry', params=params)
        return gdf


    def load_road_data(self):
        road_network_query = f"SELECT * FROM bike_road_network_{self.campaign}"

        gdf = gpd.read_postgis(road_network_query, self.db_engine, geom_col="geometry")
        if len(gdf) == 0:
            raise ProcessorExecuteError("No road network data found")
        return gdf

    def create_collection_entries(self, collection_prefix):
        if self.campaign is None and self.t_start is None and  self.t_end is None:
            self.title = f"""{collection_prefix}"""
        elif self.campaign is None and self.t_start is not None and self.t_end is not None:
            self.title = f"""{collection_prefix}_{self.t_start.split('T')[0].replace('-', '') }_{self.t_end.split('T')[0].replace('-', '') }"""
        elif self.campaign is not None and self.t_start is None and self.t_end is None:
            self.title = f"""{collection_prefix}_{self.campaign}"""
        elif self.campaign is not None and self.t_start is not None and self.t_end is not None:
            self.title = f"""{collection_prefix}_{self.campaign}_{self.t_start.split('T')[0].replace('-', '') }_{self.t_end.split('T')[0].replace('-', '') }"""
        else:
            self.title = f"""{collection_prefix}_NOINFO"""

    def read_config(self):
        with open(self.config_file, "r") as file:
            LOGGER.debug("read config")
            return yaml.safe_load(file)

    def write_config(self, new_config):
        with open(self.config_file, "w") as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")

    def update_config(self):
        lock = FileLock(f"{self.config_file}.lock")

        if self.data is not None and isinstance(self.data, gpd.GeoDataFrame):
            min_x, min_y, max_x, max_y = [float(val) for val in self.data.total_bounds]
        else:
            min_x, min_y, max_x, max_y = [-90, -180, 90, 180]

        with lock:
            config = self.read_config()
            config["resources"][f"{self.title}"] = {
                "type": "collection",
                "title": f"{self.title}",
                "description": f"{self.title}",
                "keywords": ["country"],
                "extents": {
                    "spatial": {
                        "bbox": [min_x, min_y, max_x, max_y],
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/4326",
                    },
                },
                "providers": [
                    {
                        "type": "feature",
                        "name": "PostgreSQL",
                        "data": {
                            "host": self.db_host,
                            "port": self.db_port,
                            "dbname": self.db_name,
                            "user": self.db_user,
                            "password": self.db_password,
                            "search_path": ["public"],
                        },
                        "id_field": f"{self.id_field}",
                        "table": f"{self.title}",
                        "geom_field": "geometry",
                    }
                ],
            }

            self.write_config(config)