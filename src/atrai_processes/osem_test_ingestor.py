import os
import sys
import logging
import pandas as pd
from opensensemaptoolbox import OpenSenseMap
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

#get the directory of the current script 
script_dir = os.path.dirname(__file__)

#path to the project root atrai-bikes-platform)
#go up two levels from atrai_processes -> src -> atrai-bikes-platform
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))

#add the project root to sys.path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# print("Current sys.path", sys.path)
from sqlalchemy import text, exc, inspect
from src.config.db_config import DatabaseConfig
import datetime as dt
import time
import geopandas as gpd 
from typing import Tuple, List, Union


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
        #ingest multiple tags
        "tags": {
            "title": "tags",
            "description": "List of tags to filter and ingest data for",
            "schema": {
                "type": "array", 
                "items": {"type", "string"}
                },
        },
        #old
        # "tag": {
        #     "title": "tag",
        #     "description": "tag to filter and ingest data for",
        #     "schema": {"type": "string"},
        # },
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
        #which tags were processed
        "ingested_tags": { 
            "title": "Ingested tags",
            "description": "List of tags for which data was ingested.",
            "schema": {"type": "array", "items": {"type": "string"}}
        },
        #total records count
        "total_records_ingested": { 
            "title": "Total Records Ingested",
            "description": "Total number of records ingested across all tags.",
            "schema": {"type": "integer"}
        }
    },
    # "example": {"inputs": {"token": "ABC123XYZ666", "tag": "tag"}},
    "example": {"inputs": {"token": "ABC123XYZ666", "tags": ["muenster", "wiesbaden"]}},
}


class OsemDataIngestion(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get("INT_API_TOKEN", "token")
        self.data_base_dir = "/pygeoapi/data" #postgres server connection
        self.db_config = DatabaseConfig()
        self.table_name = "osem_bike_data"
        self.schema_name = "public"
    
    def _ensure_table_and_columns_exist(self, engine): 
        retries = 5
        for i in range(retries): 
            try: 
                with engine.begin() as conn: 
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {self.schema_name}.{self.table_name} (
                            id SERIAL PRIMARY KEY, 
                            "createdAt" TIMESTAMP WITH TIME ZONE, 
                            grouptag TEXT, --Ensure grouptag column exists
                            geometry GEOMETRY(Point, 4326) --Define geometry column
                        )
                    """))
                    LOGGER.info(f"Ensured {self.schema_name}.{self.table_name} table exists")

                    try: 
                        #add grouptag and createdAt cols if do not exist
                        conn.execute(text(f"ALTER TABLE {self.schema_name}.{self.table_name} ADD COLUMN IF NOT EXISTS grouptag TEXT"))
                        conn.execute(text(f"ALTER TABLE {self.schema_name}.{self.table_name} ADD COLUMN IF NOT EXISTS \"createdAt\" TIMESTAMP WITH TIME ZONE"))
                        LOGGER.info("Ensured 'grouptag' and 'createdAt' columns exist.")
                    except exc.OperationalError as e:
                        LOGGER.warning(f"Attempt {i+1}/{retries}: Could not add/index grouptag column (likely already exists or concurrent ALTER): {e}")
                    #create indexes (now that columns are guaranteed to exist)
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_osem_bike_data_grouptag ON {self.schema_name}.{self.table_name} (grouptag)"))
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_osem_bike_data_createdat ON {self.schema_name}.{self.table_name} (\"createdAt\")"))
                    LOGGER.info(f"Ensured {self.schema_name}.{self.table_name} indexes exist.")

                    conn.commit() #commit the DDL changes
                    LOGGER.info(f"Ensured {self.schema_name}.{self.table_name} table and essential columns/indexes exist.")
                    return #success, exit function
                        
            except (exc.OperationalError, exc.ProgrammingError) as e:
                LOGGER.warning(f"Attempt {i+1}/{retries}: Database error during _ensure_table_and_columns_exist: {e}")
                if i < retries - 1:
                    time.sleep(2**i)
                else:
                    LOGGER.error(f"Failed to ensure table schema after {retries} attempts.")
                    raise ProcessorExecuteError(f"Database schema setup failed: {e}")
            except Exception as e:
                LOGGER.error(f"Unexpected error during _ensure_table_and_columns_exist: {e}")
                raise ProcessorExecuteError(f"Database schema setup failed: {e}")
            
    #change so _process_and_insert_data now processes a single gdf, ensures dynamic columns exist before inserting data
    def _process_and_insert_data(self, engine, gdf: gpd.GeoDataFrame, current_tag: str):
        
        if gdf.empty:
            LOGGER.info(f"No GeoDataFrame to insert for tag '{current_tag}'.")
            return

        LOGGER.info(f"Preparing to insert {len(gdf)} records for tag '{current_tag}'.")
        
        try: 
            with engine.begin() as conn: 
                #get existing columns from the db table
                inspector = inspect(conn) #use conn for inspector
                db_columns = [col['name'] for col in inspector.get_columns(self.table_name, schema=self.schema_name)]

                #identify new columns in the GeoDataFrame that are not in the db, exclude gdf cols already handled in osmtoolbox
                columns_to_ignore = {'geometry', 'grouptag', 'id', 'createdAt',
                                     'phenomenon', 'unit', 'interval', 'lastMeasurement',
                                     'loc', 'lat', 'lon', 'altitude', 'date'} 
                new_columns = [
                    col for col in gdf.columns
                    if col not in db_columns and col not in columns_to_ignore
                ]

                #add new columns to the table with DOUBLE PRECISION type
                if new_columns:
                    LOGGER.info(f"Adding new columns to table {self.table_name} for tag '{current_tag}': {new_columns}")
                    for col in new_columns: 
                        add_column_statements = f'ALTER TABLE {self.schema_name}.{self.table_name} ADD COLUMN "{col}" DOUBLE PRECISION'
                        try: 
                            conn.execute(text(add_column_statements))
                            LOGGER.info(f"Successfully executed DDL: {add_column_statements}")
                        except Exception as add_col_e:
                            #handle cases where column might have been added by another process 
                            if "already exists" in str(add_col_e).lower():
                                LOGGER.warning(f"Column '{col}' already exists for tag '{current_tag}', skipping ALTER TABLE: {add_col_e}")
                            else:
                                LOGGER.error(f"Failed to add column {col} for tag '{current_tag}': {add_col_e}")
                                raise 
                #ensure 'createdAt' is datetime with UTC timezone
                if 'createdAt' in gdf.columns:
                    #convert to datetime, coerce errors (empty strings) to NaT
                    gdf['createdAt'] = pd.to_datetime(gdf['createdAt'], utc=True, errors='coerce')
                    #drop rows where createdAt conversion failed if desired, or handle NaT during insertion
                    gdf = gdf.dropna(subset=['createdAt'])
                    if gdf.empty:
                        LOGGER.warning(f"No valid 'createdAt' timestamps found after conversion for tag '{current_tag}'. Skipping insertion.")
                        return

                #ensure all sensor value columns are numeric (coerce errors to NaN)
                #re-fetch db cols after potential ALTERs to ensure only process existing columns
                db_columns_after_alter = [col['name'] for col in inspector.get_columns(self.table_name, schema=self.schema_name)]

                for col in gdf.columns:
                    if col in db_columns_after_alter and col not in columns_to_ignore:
                        if not pd.api.types.is_numeric_dtype(gdf[col]):
                            gdf[col] = pd.to_numeric(gdf[col], errors='coerce') #convert non-numeric to NaN

                #set CRS and insert data into postgis
                gdf = gdf.set_crs('epsg:4326', allow_override=True)

                #filter gdf to only include cols that exist in the db after ALTERs, prevents errors if gdf has temp columns not meant for db
                final_cols = [col for col in gdf.columns if col in db_columns_after_alter]
                gdf_to_insert = gdf[final_cols]

                retries = 3
                for i in range(retries):
                    try:
                        gdf_to_insert.to_postgis(
                            name=self.table_name,
                            # con=engine,
                            con=conn,
                            schema=self.schema_name,
                            if_exists='append',
                            index=False
                        )
                        LOGGER.info(f"Successfully inserted {len(gdf_to_insert)} records for tag '{current_tag}'.")
                        return #success, break out of retry loop
                    except (exc.OperationalError, exc.ProgrammingError) as e:
                        LOGGER.warning(f"Attempt {i+1}/{retries}: Database error during data insertion for tag '{current_tag}': {e}")
                        if i < retries - 1:
                            time.sleep(2**i) 
                        else:
                            LOGGER.error(f"Failed to insert data for tag '{current_tag}' after {retries} attempts: {e}")
                            raise ProcessorExecuteError(f"Data insertion for tag '{current_tag}' failed after retries: {e}")
                    except Exception as e:
                        LOGGER.error(f"An unexpected error occurred during data insertion for tag '{current_tag}': {e}", exc_info=True)
                        raise ProcessorExecuteError(f"Data insertion for tag '{current_tag}' failed: {e}")

        except Exception as e:
            LOGGER.error(f"Error processing and inserting data for tag '{current_tag}': {e}", exc_info=True)
            raise 
                

        # for gdf in gdfs:
        #     self._add_missing_columns(engine, gdf)
        #     gdf = gdf.set_crs('epsg:4326', allow_override=True)
        #     gdf.to_postgis("osem_bike_data", engine, if_exists="append", index=False)

        # # Create id column and set as primary key
        # with engine.begin() as conn:
        #     conn.execute(text("ALTER TABLE osem_bike_data DROP COLUMN IF EXISTS id"))
        #     conn.execute(text("ALTER TABLE osem_bike_data ADD COLUMN id SERIAL PRIMARY KEY"))

    # def _add_missing_columns(self, engine, gdf):
    #     columns = gdf.columns.tolist()
    #     with engine.begin() as conn:
    #         existing_columns = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='osem_bike_data'")).fetchall()
    #         existing_columns = [col[0] for col in existing_columns]
    #         new_columns = [col for col in columns if col not in existing_columns]

    #         for col in new_columns:
    #             conn.execute(text(f'ALTER TABLE osem_bike_data ADD COLUMN "{col}" DOUBLE PRECISION'))

    def execute(self, data: dict) -> Tuple[str, dict]:
        mimetype = "application/json"
        total_ingested_records = 0 
        ingested_tags_list = []
        overall_status = "success"
        overall_message = "Data ingestion completed for all tags"
        self.token = data.get("token")

        #get list of tags, default "bike" if not provided 
        self.tags_to_ingest: List[str] = data.get("tags")
        if not self.tags_to_ingest:
            LOGGER.warning("No 'tags' provided in input data. Defaulting to ['bike'].")
            self.tags_to_ingest = ["bike"]
        elif not isinstance(self.tags_to_ingest, list):
            raise ProcessorExecuteError("Input 'tags' must be a list of strings.")

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        engine = None
        try:
            engine = self.db_config.get_engine()
            #ensure base table and common cols exist once at start
            self._ensure_table_and_columns_exist(engine)

            for current_tag in self.tags_to_ingest:
                LOGGER.info(f"--- Starting ingestion process for campaign tag: '{current_tag}' ---")
                try:
                    #get latest date from the db for the specific tag with retry logic
                    latest_date_iso = '1970-01-01T00:00:00Z'
                    retries = 3
                    for i in range(retries):
                        try:
                            with engine.connect() as conn:
                                #ensure table exists and cols are ready before querying MAX(createdAt)
                                sql_query = text(f'SELECT MAX("createdAt") FROM {self.schema_name}.{self.table_name} WHERE "grouptag" = :tag')
                                result = conn.execute(sql_query, {"tag": current_tag}).fetchone()

                                if result and result[0]:
                                    latest_date_dt = result[0]
                                    if latest_date_dt.tzinfo is None:
                                        latest_date_dt = latest_date_dt.replace(tzinfo=dt.timezone.utc)
                                    else:
                                        latest_date_dt = latest_date_dt.astimezone(dt.timezone.utc)
                                    latest_date_iso = latest_date_dt.isoformat().replace("+00:00", "Z")
                                    LOGGER.info(f"Latest date for tag '{current_tag}' from DB: {latest_date_iso}")
                                    break 
                        except (exc.OperationalError, exc.ProgrammingError) as e:
                            LOGGER.error(f"Attempt {i+1}/{retries}: Error getting latest date for tag '{current_tag}': {e}")
                            if i < retries - 1:
                                time.sleep(2**(i+1)) 
                            else:
                                LOGGER.error(f"Failed to get latest date for tag '{current_tag}' after {retries} attempts. Proceeding with initial date. Error: {e}")
                                break #continue with 1970-01-01 if all retries fail
                        except Exception as e:
                            LOGGER.error(f"Unexpected error getting latest date for tag '{current_tag}': {e}", exc_info=True)
                            break #break on unexpected errors

                    # Query data from osm
                    osm = OpenSenseMap()
                    today_iso = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
                    LOGGER.info(f"Querying OSM for tag '{current_tag}' from {latest_date_iso} to {today_iso}")
                    #use box_sensor_dict_by_tag to get the relevant box IDs for the given tag
                    boxes_sensors_list = osm.box_sensor_dict_by_tag(current_tag)
                    #extract unique box IDs from the returned list of dictionaries
                    box_ids_to_add = list(set([item['boxId'] for item in boxes_sensors_list]))

                    if not box_ids_to_add:
                        LOGGER.warning(f"No boxes found for tag '{current_tag}' via box_sensor_dict_by_tag. Nothing to ingest for this tag.")
                        continue #move to the next tag
                    LOGGER.info(f"Adding {len(box_ids_to_add)} boxes to OpenSenseMap object for tag '{current_tag}'.")
                    #add identified boxes to the OpenSenseMap instance
                    osm.add_box(box_ids_to_add)

                    LOGGER.info(f"Fetching sensor data for all relevant boxes for tag '{current_tag}' from {latest_date_iso} to {today_iso}.")
                    osm.fetch_box_data(t_from=latest_date_iso, t_to=today_iso) #use t_from and t_to from APIressources/Box/Sensor

                    #manually aggregate gdfs
                    all_gdfs_for_tag = []
                    for box_obj in osm.boxes:
                        #box_obj.data_fetched holds the merged gdf for that specific box after fetch_box_data
                        if box_obj.data_fetched is not None and not box_obj.data_fetched.empty:
                            #add the 'grouptag' column to each individual box's gdf before concat, ensures tag is present when inserting into the db
                            box_obj.data_fetched['grouptag'] = current_tag
                            all_gdfs_for_tag.append(box_obj.data_fetched)
                        else:
                            LOGGER.info(f"No data fetched for box '{box_obj.boxId}' (tag: {current_tag}) within specified date range or no sensors/data.")

                    if not all_gdfs_for_tag:
                        LOGGER.info(f"No GeoDataFrames to insert for tag '{current_tag}' after fetching and aggregation.")
                        continue #move to the next tag

                    #concatenate all collected GeoDataFrames into a single one for batch insertion & set crs
                    merged_gdfs_for_insertion = gpd.GeoDataFrame(pd.concat(all_gdfs_for_tag, ignore_index=True), crs=all_gdfs_for_tag[0].crs)
                    LOGGER.info(f"Aggregated {len(merged_gdfs_for_insertion)} total records for tag '{current_tag}'.")

                    #process and insert data for the current tag
                    self._process_and_insert_data(engine, merged_gdfs_for_insertion, current_tag)
                    total_ingested_records += len(merged_gdfs_for_insertion)
                    ingested_tags_list.append(current_tag)
                    LOGGER.info(f"Finished ingestion for campaign tag: '{current_tag}'")

                except Exception as tag_e:
                    LOGGER.error(f"Error ingesting data for tag '{current_tag}'. Skipping to next tag if available. Error: {tag_e}", exc_info=True)
                    overall_status = "partial_success"
                    overall_message = "Some tags failed to ingest data fully."
                    #no re-raise here, allow the loop to continue for other tags

            # outputs = {"id": "osem_data_ingestion", "status": f'''ingested {len(gdfs)} boxes for tag'{self.tag}' '''}
            # return mimetype, outputs
                    
            outputs = {
                "id": "osem_data_ingestion",
                "status": overall_status,
                "ingested_tags": ingested_tags_list,
                "total_records_ingested": total_ingested_records,
                "message": overall_message
            }
            return mimetype, outputs

        except Exception as e:
            LOGGER.error(f"Overall data ingestion process failed: {e}", exc_info=True)
            raise #re-raise for other errors (maybe db connection)

        finally:
            if engine:
                engine.dispose() #ensure connections are closed

    
        # # Get the latest date from the database
        # with engine.begin() as conn:
        #     latest_date = '1970-01-01T00:00:00Z'
        #     try:
        #         result = conn.execute(text('SELECT MAX("osem_bike_data"."createdAt") FROM "osem_bike_data" WHERE grouptag ...')).fetchone()
        #         # format the date to be compatible with the OpenSenseMap API
        #         latest_date = result[0].astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        #     except Exception as e:
        #         LOGGER.error(f"Error getting latest date from database: {e}")

        # Delete existing data
        # self._clear_existing_data(engine)

        # Process and insert data
        # self._process_and_insert_data(engine, gdfs)

        # outputs = {"id": "osem_data_ingestion", "status": f'''ingested {len(gdfs)} boxes for tag'{self.tag}' '''}
        # return mimetype, outputs
    
    # def _clear_existing_data(self, engine):
    #     with engine.begin() as conn:
    #         conn.execute(text("DELETE FROM osem_bike_data"))
    #         conn.execute(text("ALTER TABLE osem_bike_data ALTER COLUMN geometry TYPE geometry(Point, 4326) USING ST_SetSRID(geometry, 4326)"))
    

    def __repr__(self):
        return f"<OsemDataIngestion> {self.name}"

#for testing 
if __name__ == "__main__":
    #log for testing
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    LOGGER.setLevel(logging.INFO) #get info

    #create processor instance
    processor = OsemDataIngestion(processor_def={"name": "statistics"}) 

    # ##test multiple tags
    # input_data_multiple_tags = {
    #     "token": os.environ.get("INT_API_TOKEN", "token"), 
    #     "tags": ["muenster", "wiesbaden"] #tags to test
    # }

    # LOGGER.info("\nStarting OsemDataIngestion process for multiple tags...")
    # try:
    #     mimetype, outputs = processor.execute(input_data_multiple_tags)
    #     LOGGER.info(f"Overall process completed. Status: {outputs.get('status')}")
    #     LOGGER.info(f"Ingested Tags: {outputs.get('ingested_tags')}")
    #     LOGGER.info(f"Total Records Ingested: {outputs.get('total_records_ingested')}")
    #     LOGGER.info(f"Mimetype: {mimetype}")
    #     LOGGER.info(f"Message: {outputs.get('message')}")
    # except ProcessorExecuteError as e:
    #     LOGGER.error(f"Processor execution error: {e}")
    # except Exception as e:
    #     LOGGER.error(f"An unexpected error occurred during process execution: {e}", exc_info=True)
    
    #test muenster campaign 
    input_data_muenster = {
        "token": os.environ.get("INT_API_TOKEN"), 
        "tags": ["muenster"]
    }

    LOGGER.info("\nStarting OsemDataIngestion process for tag 'muenster'..")
    try: 
        mimetype, outputs = processor.execute(input_data_muenster)
        LOGGER.info(f"Process for tag 'muenster' completed successfully. Status: {outputs.get('status')}")
        LOGGER.info(f"Ingested Tags: {outputs.get('ingested_tags')}")
        LOGGER.info(f"Mimetype: {mimetype}")
    except ProcessorExecuteError as e:
        LOGGER.error(f"Processor execution error: {e}")
    except Exception as e:
        LOGGER.error(f"An unexpected error occurred during process execution: {e}", exc_info=True)
    
    
    