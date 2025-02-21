import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd
import folium
import geopandas as gpd
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import numpy as np
from sklearn.neighbors import BallTree
from sqlalchemy import create_engine

from .html_helper import create_speed_legend_html, create_traffic_flow_legend_html
from .useful_functs import filter_bike_data_location, nearest_neighbor_search

LOGGER = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:postgres@postgis:5432/geoapi_db"

METADATA = {
    'version': '0.2.0',
    'id': 'speed_traffic_flow',
    'title': {
        'en': 'speed_traffic_flow',
    },
    'description': {
        'en': 'evaluates speed and traffic flow'},
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

def filter_start_end(group):
    standing_threshold = 0.9
    group = group.copy()

    start_indices_to_drop = []
    for idx, row in group.iterrows():
        if row['Standing'] > standing_threshold:
            start_indices_to_drop.append(idx)
        else:
            break

    end_indices_to_drop = []
    for idx, row in group[::-1].iterrows():
        if row['Standing'] > standing_threshold:
            end_indices_to_drop.append(idx)
        else:
            break

    group = group.drop(index=start_indices_to_drop + end_indices_to_drop)

    return group

class SpeedTrafficFlow(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, METADATA)
        self.secret_token = os.environ.get('INT_API_TOKEN', 'token')
        self.data_base_dir = '/pygeoapi/data'
        self.html_out = '/pygeoapi/data/html'

    def execute(self, data):
        mimetype = 'application/json'

        self.boxid = data.get('id')
        self.token = data.get('token')

        if self.boxid is None:
            raise ProcessorExecuteError('Cannot process without a id')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        
        atrai_bike_data = pd.read_csv('/pygeoapi/combined_data.csv')
        device_counts = atrai_bike_data.groupby('device_id').size()
        valid_device_ids = device_counts[device_counts >= 10].index
        atrai_bike_data = atrai_bike_data[atrai_bike_data['device_id'].isin(valid_device_ids)]
        
        engine = create_engine(DB_URL)
        road_network_query = "SELECT * FROM muenster_bike_roads"
        edges_filtered = gpd.read_postgis(road_network_query, engine, geom_col='geometry')

        filtered_data_MS = filter_bike_data_location(atrai_bike_data)

        filtered_data_MS = filtered_data_MS[['createdAt', 'Speed', 'lat', 'lng', 'device_id', 'Standing']]
        filtered_data_MS['createdAt'] = pd.to_datetime(filtered_data_MS['createdAt'])
        filtered_data_MS = filtered_data_MS[filtered_data_MS['Speed'] >= 0]
        percentile_999 = filtered_data_MS['Speed'].quantile(0.999)
        filtered_data_MS['Normalized_Speed'] = (filtered_data_MS['Speed'] / percentile_999).clip(upper=1)

        edges_filtered = edges_filtered.reset_index(drop=True)
        filtered_data_MS = nearest_neighbor_search(filtered_data_MS, edges_filtered)

        segment_data = filtered_data_MS.groupby('road_segment').agg(
            avg_speed=('Normalized_Speed', 'mean'),
            points_in_segment=('road_segment', 'size')
        ).reset_index()

        cmap = cm.get_cmap("plasma").reversed()
        m_lines = folium.Map(location=[51.9607, 7.6261], zoom_start=12, tiles="Cartodb dark_matter", attr="Map tiles by CartoDB, under CC BY 3.0. Data by OpenStreetMap, under ODbL.")
        segment_data['avg_speed_unnorm_kmh'] = (segment_data['avg_speed'] * percentile_999) * 3.6

        for _, row in segment_data.iterrows():
            road_segment_idx = row['road_segment']
            road_segment = edges_filtered.loc[road_segment_idx]
            
            if not road_segment.geometry.is_empty:
                line = road_segment.geometry
                color = mcolors.to_hex(cmap(row['avg_speed']))
                tooltip_text = f"Data Points: {row['points_in_segment']}<br>Avg Speed: {row['avg_speed_unnorm_kmh']:.2f} km/h"
                    
                folium.PolyLine(
                        locations=[(lat, lng) for lng, lat in line.coords],
                        color=color,
                        weight=4,
                        tooltip=folium.Tooltip(tooltip_text)
                    ).add_to(m_lines)


        legend_html_speed = create_speed_legend_html(segment_data, cmap)
        m_lines.get_root().html.add_child(folium.Element(legend_html_speed))

        os.makedirs(self.html_out, exist_ok=True)
        m_lines.save(os.path.join(self.html_out, "speed_map.html"))

        filtered_data_MS_tf = filter_bike_data_location(atrai_bike_data)
        filtered_data_MS_tf = filtered_data_MS_tf[['createdAt', 'Speed', 'lat', 'lng', 'device_id', 'Standing']]
        filtered_data_MS_tf['createdAt'] = pd.to_datetime(filtered_data_MS_tf['createdAt'])
        filtered_data_MS_tf = filtered_data_MS_tf.dropna(subset=['Standing'])
        filtered_data_MS_tf = filtered_data_MS_tf.sort_values(by='createdAt')
        filtered_data_MS_tf['time_diff'] = filtered_data_MS_tf.groupby('device_id')['createdAt'].diff().dt.total_seconds() / 60
        filtered_data_MS_tf['new_ride'] = filtered_data_MS_tf['time_diff'] > 10
        filtered_data_MS_tf['ride_id'] = filtered_data_MS_tf.groupby('device_id')['new_ride'].cumsum() + 1

        filtered_data_MS_tf = filtered_data_MS_tf.groupby('ride_id').apply(lambda group: filter_start_end(group)).reset_index(drop=True)
        
        filtered_data_MS_tf = filtered_data_MS_tf[filtered_data_MS_tf['Speed'] >= 0]
        percentile_999_tf = filtered_data_MS_tf['Speed'].quantile(0.999)
        filtered_data_MS_tf['Normalized_Speed'] = (filtered_data_MS_tf['Speed'] / percentile_999_tf).clip(upper=1)
        filtered_data_MS_tf['traffic_flow'] = (filtered_data_MS_tf['Normalized_Speed'] * (1 - (filtered_data_MS_tf['Standing'] ** 2)))

        edges_filtered = edges_filtered.reset_index(drop=True)
        filtered_data_MS_tf = nearest_neighbor_search(filtered_data_MS_tf, edges_filtered)

        segment_data_tf = filtered_data_MS_tf.groupby('road_segment').agg(
            avg_traffic_flow=('traffic_flow', 'mean'),
            points_in_segment=('road_segment', 'size')
        ).reset_index()
        
        cmap = cm.get_cmap('RdYlGn')
        norm = mcolors.Normalize(vmin=0.3, vmax=0.7)

        m_traffic_flow = folium.Map(location=[51.9607, 7.6261], zoom_start=12,  tiles="Cartodb dark_matter", attr="Map tiles by CartoDB, under CC BY 3.0. Data by OpenStreetMap, under ODbL.")

        for _, row in segment_data_tf.iterrows():
            road_segment_idx = row['road_segment']
            road_segment = edges_filtered.loc[road_segment_idx]
            
            if not road_segment.geometry.is_empty:
                line = road_segment.geometry
                color = mcolors.to_hex(cmap(norm(row['avg_traffic_flow'])))
                tooltip_text = f"Data Points: {row['points_in_segment']}<br>Avg Traffic Flow: {row['avg_traffic_flow']:.2f}"
                
                folium.PolyLine(
                    locations=[(lat, lng) for lng, lat in line.coords],
                    color=color,
                    tooltip=folium.Tooltip(tooltip_text),
                    weight=4,
                ).add_to(m_traffic_flow)

        legend_html_traffic_flow = create_traffic_flow_legend_html(segment_data_tf, cmap)
        m_traffic_flow.get_root().html.add_child(folium.Element(legend_html_traffic_flow))

        m_traffic_flow.save(os.path.join(self.html_out, "traffic_flow_map.html"))
                
        outputs = {
            'id': 'speed_traffic_flow',
            'status': f"""created html at '{os.path.join(self.html_out, "speed_map.html")}' and '{os.path.join(self.html_out, "traffic_flow_map.html")}'"""
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<SpeedTrafficFlow> {self.name}'