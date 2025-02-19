import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd
from opensensemaptoolbox import OpenSenseMap
import folium
import osmnx as ox
from shapely.geometry import Point, LineString
import geopandas as gpd
import numpy as np
from sklearn.neighbors import BallTree
import matplotlib.colors as mcolors
import matplotlib.cm as cm

from .html_helper import create_distances_legend_html
from .useful_functs import filter_bike_data_location, nearest_neighbor_search

LOGGER = logging.getLogger(__name__)

METADATA = {
    'version': '0.2.0',
    'id': 'Distances',
    'title': {
        'en': 'Distances',
    },
    'description': {
        'en': 'Distances from overtaking manoeuvres'},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['process'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'id': {
            'title': 'boxid',
            'description': 'boxid to get the data from',
            'schema': {
                'type': 'string'
            }
        },
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        }
    },
    'outputs': {
        'id': {
            'title': 'ID',
            'description': 'The ID of the process execution',
            'schema': {
                'type': 'string'
            }
        },
        'status': {
            'title': 'status',
            'description': 'describes process',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "ids": "ABCDEF123456",
            "token": "ABC123XYZ666"
        }
    }
}

class Distances(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get('INT_API_TOKEN', 'token')
        self.data_base_dir = '/pygeoapi/data'
        self.html_out = '/pygeoapi/data/html'


    def execute(self, data):
        mimetype =  'application/json'

        self.boxid = data.get('id')
        self.token = data.get('token')

        if self.boxid is None:
            raise ProcessorExecuteError('Cannot process without a id')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        # if self.boxid not in os.listdir(self.data_base_dir):
        #     LOGGER.info(f'download data for {self.boxid}')
        #     OSM = OpenSenseMap.OpenSenseMap()
        #     OSM.add_box(self.boxid)
        #     OSM.save_OSM()

        #script
        atrai_bike_data = pd.read_csv('/pygeoapi/combined_data.csv')
        device_counts = atrai_bike_data.groupby('device_id').size()
        valid_device_ids = device_counts[device_counts >= 10].index
        atrai_bike_data = atrai_bike_data[atrai_bike_data['device_id'].isin(valid_device_ids)]

        #probably would be good to have in database:
        road_network_muenster = ox.graph_from_place("Münster, Germany", network_type='bike')
        nodes, edges = ox.graph_to_gdfs(road_network_muenster)
        # Filter out major roads
        edges_filtered = edges[~edges['highway'].isin(['primary', 'secondary', 'tertiary'])]

        filtered_data_MS = filter_bike_data_location(atrai_bike_data)

        filtered_data_MS = filtered_data_MS[['createdAt', 'lat', 'lng', 'device_id', 'Overtaking Distance', 'Overtaking Manoeuvre']]
        filtered_data_MS['createdAt'] = pd.to_datetime(filtered_data_MS['createdAt'])
        filtered_data_MS = filtered_data_MS.dropna(subset=["Overtaking Distance"])
        filtered_data_MS = filtered_data_MS[filtered_data_MS["Overtaking Manoeuvre"] > 0.05]
        filtered_data_MS = filtered_data_MS[filtered_data_MS["Overtaking Distance"] > 0]
        filtered_data_MS['Normalized Overtaking Distance'] = (atrai_bike_data['Overtaking Distance'] / 200).clip(upper=1) 

        edges_filtered = edges_filtered.reset_index(drop=True)
        filtered_data_MS = nearest_neighbor_search(filtered_data_MS, edges_filtered)

        segment_data = filtered_data_MS.groupby('road_segment').agg(
            avg_dist=('Normalized Overtaking Distance', 'mean'),
            points_in_segment=('road_segment', 'size')
        ).reset_index()

        #segment_data = segment_data[segment_data['points_in_segment'] > 5]

        cmap = cm.get_cmap("RdYlGn")

        m_distance = folium.Map(location=[51.9607, 7.6261], zoom_start=12, tiles="Cartodb dark_matter", attr="Map tiles by CartoDB, under CC BY 3.0. Data by OpenStreetMap, under ODbL.")
        segment_data['avg_distance_unnorm'] = (segment_data['avg_dist'] * 200)

        for _, row in segment_data.iterrows():
            road_segment_idx = row['road_segment']
            road_segment = edges_filtered.loc[road_segment_idx]
            
            if not road_segment.geometry.is_empty:
                line = road_segment.geometry
                color = mcolors.to_hex(cmap(row['avg_dist']))
                tooltip_text = f"Data Points: {row['points_in_segment']}<br>Avg Distance: {row['avg_distance_unnorm']:.2f} cm"
                    
                folium.PolyLine(
                        locations=[(lat, lng) for lng, lat in line.coords],
                        color=color,
                        weight=4,
                        tooltip=folium.Tooltip(tooltip_text)
                    ).add_to(m_distance)
                
        legend_html_distances = create_distances_legend_html(segment_data, cmap)
        m_distance.get_root().html.add_child(folium.Element(legend_html_distances))

        os.makedirs(self.html_out, exist_ok=True)
        m_distance.save(os.path.join(self.html_out, "distances_flowmap.html"))

        outputs = {
            'id': 'distances_flowmap',
            'status': f"""created html at '{os.path.join(self.html_out, "distances_flowmap.html")}'"""
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<Distances> {self.name}'