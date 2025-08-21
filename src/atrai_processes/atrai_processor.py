import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from sqlalchemy import create_engine, text
import pandas as pd
import geopandas as gpd
import datetime

LOGGER = logging.getLogger(__name__)


class AtraiProcessor(BaseProcessor):
    def __init__(self, processor_def, METADATA):
        super().__init__(processor_def, METADATA)

        self.secret_token = os.environ.get('INT_API_TOKEN', 'token')
        self.data_base_dir = os.environ.get('BASE_DATA_DIR')
        self.html_out = os.environ.get('HTML_OUT_DIR')
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

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        if not self.boxId and not self.campaign:
            LOGGER.error("neither campaign nor boxid provided")
            raise ProcessorExecuteError('neither campaign nor boxid provided - cannot continue')

        if self.boxId and not isinstance(self.boxId, list):
            LOGGER.error("boxId needs to be a list of boxIds - can be a single one")
            raise ProcessorExecuteError("boxId needs to be a list of boxIds - can be a single one")

        if self.campaign and self.campaign not in self.metatable['location'].values:
            LOGGER.error(f""" '{self.campaign}' is not in metatable. Valid values are '{", ".join([c for c in self.metatable['location'].unique()])}' """)
            raise ProcessorExecuteError(f""" '{self.campaign}' is not in metatable. Valid values are '{", ".join([c for c in self.metatable['location'].unique()])}' """)

        if self.boxId and not all(i in self.metatable['id'].values for i in self.boxId):
            LOGGER.error(f""" ""'{self.boxId}' is not in metatable. Valid values are {", ".join([c for c in self.metatable['id'].unique()])} """)
            raise ProcessorExecuteError(f"""'{self.boxId}' is not in metatable. Valid values are {", ".join([c for c in self.metatable['id'].unique()])} """)
        if self.campaign and self.boxId:
            self.campaign = None

        if self.t_start and self.t_end:
            if datetime.datetime.fromisoformat(self.t_start) >= datetime.datetime.fromisoformat(self.t_end):
                LOGGER.error(f"t_start: '{self.t_start}' is bigger than t_end: '{self.t_end}'")
                raise ProcessorExecuteError(f"t_start: '{self.t_start}' is bigger than t_end: '{self.t_end}'")


    def load_data(self):
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

