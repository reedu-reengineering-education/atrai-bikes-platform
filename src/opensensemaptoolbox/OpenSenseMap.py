import os
import requests
import pandas as pd
import geopandas as gpd
import numpy
import json
from io import StringIO
from urllib.parse import urljoin

from .APIressources import APIressources
from .Box import Box


class OpenSenseMap(APIressources):
    def __init__(self):
        self.boxes = []
        super().__init__(endpoints={
            'tags': {
                'endpoint': '/tags',
                'ref': 'https://docs.opensensemap.org/#api-Boxes-getAllTags'
            },
            'box_data_bytag': {
                'endpoint': '/boxes',
                'ref': 'https://docs.opensensemap.org/#api-Measurements-getDataByGroupTag'
            }
        })

    def get_tags(self):
        data = self.get_data((self.endpoint_merge('tags')))
        return data


    def box_sensor_dict_by_tag(self, tag: str):
        data = self.get_data(self.endpoint_merge('box_data_bytag'), params=dict(grouptag=tag), format='json')
        box_ids = set(sorted([item['_id'] for item in data]))
        for box_id in box_ids:
            box = Box(box_id)
            box.data = box.get_box_data()
            self.add_box(box)

        boxes_sensors = [dict(boxId=box_id,
                      sensorId=list(set(sorted([sensor['_id'] for item in data if box_id == item['_id'] for sensor in item['sensors']])))
                 ) for box_id in box_ids]
        return boxes_sensors

    def add_box(self, boxId):
        if isinstance(boxId, str):
            box = Box(boxId)
            self.boxes.append(box)
        if isinstance(boxId, list):
            if isinstance(boxId[0], str):
                self.boxes = [Box(bId) for bId in boxId]
            if isinstance(boxId, Box):
                self.boxes = boxId
        if isinstance(boxId, Box):
            self.boxes.append(boxId)

    def save_OSM(self):
        data_path = './data'
        if len(self.boxes) > 0:
            for box in self.boxes:
                box_data_path = os.path.join(data_path, box.metadata["name"])
                os.makedirs(box_data_path, exist_ok=True)
                if isinstance(box.data, gpd.GeoDataFrame):
                    box.save_csv(box.data, os.path.join(box_data_path, 'data.csv'))

    def get_gdfs(self):
        output_gdfs = []
        for box in self.boxes:
            if isinstance(box.data, gpd.GeoDataFrame):
                output_gdfs.append(box.data)
        return output_gdfs