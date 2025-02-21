import os
import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pandas as pd
import folium
from folium.plugins import HeatMap

from .useful_functs import filter_bike_data_location, replace_outliers_with_nan_by_device
from .html_helper import create_danger_zones_legend_html, create_danger_zones_pm_legend_html

LOGGER = logging.getLogger(__name__)

METADATA = {
    'version': '0.2.0',
    'id': 'dangerous_places',
    'title': {
        'en': 'dangerous_places',
    },
    'description': {
        'en': 'evaluates danger of certain zones'},
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

class DangerousPlaces(BaseProcessor):
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

        atrai_bike_data = pd.read_csv('/pygeoapi/combined_data.csv')
        device_counts = atrai_bike_data.groupby('device_id').size()
        valid_device_ids = device_counts[device_counts >= 10].index
        atrai_bike_data = atrai_bike_data[atrai_bike_data['device_id'].isin(valid_device_ids)]
        filtered_data_MS = filter_bike_data_location(atrai_bike_data)

        danger_data = filtered_data_MS[['createdAt', 'Overtaking Manoeuvre', 'Overtaking Distance', 'Standing', 'Rel. Humidity', 'Finedust PM1', 'Finedust PM2.5', 'Finedust PM4', 'Finedust PM10', 'geometry', 'device_id', 'lng', 'lat']]

        danger_zones = danger_data.copy()
        max_distance = 400
        danger_zones['Normalized Distance'] = 1 - (danger_zones['Overtaking Distance'] / max_distance)
        danger_zones['Normalized Distance'] = danger_zones['Normalized Distance'].clip(lower=0, upper=1)
        danger_zones = danger_zones[danger_zones['Overtaking Manoeuvre'] > 0.05] 
        alpha = 0.3  # Weight for Overtaking Manoeuvre
        beta = 0.7  # Weight for Overtaking Distance
        danger_zones['Risk Index Overtaking'] = (alpha * danger_zones['Overtaking Manoeuvre'] +
                                                beta * danger_zones['Normalized Distance'])

        m_danger_zones = folium.Map(location=[51.9607, 7.6261], zoom_start=12)
        heatmap_data_dz = danger_zones[['lat', 'lng', 'Risk Index Overtaking']].dropna()
        
        HeatMap(
            data=heatmap_data_dz.values,
            radius=10,                   
            blur=10,                                       
        ).add_to(m_danger_zones)

        legend_html_danger_zones = create_danger_zones_legend_html(danger_zones)
        m_danger_zones.get_root().html.add_child(folium.Element(legend_html_danger_zones))

        os.makedirs(self.html_out, exist_ok=True)
        m_danger_zones.save(os.path.join(self.html_out, "danger_zones.html"))

        danger_zones_PM = danger_data.copy()
        danger_zones_PM = danger_zones_PM[(danger_zones_PM['Rel. Humidity'] <= 75) & (danger_zones_PM['Rel. Humidity'].notna())]
        pm_columns = ['Finedust PM1', 'Finedust PM2.5', 'Finedust PM4', 'Finedust PM10']
        for column in pm_columns:
            danger_zones_PM = replace_outliers_with_nan_by_device(danger_zones_PM, column)
        for column in pm_columns:
            danger_zones_PM[column] = danger_zones_PM[column].astype('float64')

        danger_zones_PM['Normalized Distance'] = 1 - (danger_zones_PM['Overtaking Distance'] / max_distance)
        danger_zones_PM['Normalized Distance'] = danger_zones_PM['Normalized Distance'].clip(lower=0, upper=1)
        pm_columns = ['Finedust PM1', 'Finedust PM2.5', 'Finedust PM4', 'Finedust PM10']
        for column in pm_columns:
            max_value = danger_zones_PM[column].max()
            normalized_column = f'Normalized {column.split()[-1]}'
            danger_zones_PM[normalized_column] = danger_zones_PM[column] / max_value

        a = 0.15  # Weight for Overtaking Manoeuvre
        b = 0.35  # Weight for Overtaking Distance
        c = 0.2  # Weight for PM1
        d = 0.15  # Weight for PM2.5
        e = 0.1  # Weight for PM4
        f = 0.05  # Weight for PM10

        danger_zones_PM['Risk Index'] = (
        a * danger_zones_PM['Overtaking Manoeuvre'] +
        b * danger_zones_PM['Normalized Distance'] +
        c * danger_zones_PM['Normalized PM1'] +
        d * danger_zones_PM['Normalized PM2.5'] +
        e * danger_zones_PM['Normalized PM4'] +
        f * danger_zones_PM['Normalized PM10'])

        m_danger_zones_PM = folium.Map(location=[51.9607, 7.6261], zoom_start=12)

        heatmap_data_danger_zones_PM = danger_zones_PM[['lat', 'lng', 'Risk Index']].dropna()

        HeatMap(
            data=heatmap_data_danger_zones_PM.values,
            radius=10,                   
            blur=10,                     
            max_zoom=1                   
        ).add_to(m_danger_zones_PM)

        legend_html_danger_zones_pm = create_danger_zones_pm_legend_html(danger_zones_PM)
        m_danger_zones_PM.get_root().html.add_child(folium.Element(legend_html_danger_zones_pm))

        m_danger_zones_PM.save(os.path.join(self.html_out, "PM_danger_zones.html"))

        outputs = {
            'id': 'dangerous_places',
            'status': f"""created html at '{os.path.join(self.html_out, "danger_zones.html")} and {os.path.join(self.html_out, "PM_danger_zones.html")}'"""
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<DangerousPlaces> {self.name}'