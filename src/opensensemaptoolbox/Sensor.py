import os
import requests
import pandas as pd
import geopandas
import numpy
import json
from io import StringIO
from urllib.parse import urljoin
import datetime as dt

from .APIressources import APIressources


class Sensor(APIressources):
    def __init__(self, boxId: str, sensorId: str, from_date=None, to_date=None):
        self.boxId = boxId
        self.sensorId = sensorId
        super().__init__(endpoints={'get_data': {
                                        'endpoint': f'/boxes/{self.boxId}/data/{self.sensorId}',
                                        'ref': 'https://docs.opensensemap.org/#api-Measurements-getData'},
                                    'sensor': {
                                        'endpoint': f'/boxes/{self.boxId}/sensors/{self.sensorId}',
                                        'ref': 'https://docs.opensensemap.org/#api-Measurements-getLatestMeasurementOfSensor'}
                                   },
        )
        self.metadata = self.get_sensor_metadata()
        self.data = self.get_sensor_data(from_date=from_date, to_date=to_date)
        self.status = {}

    def get_sensor_metadata(self):
        data = None

        try:
            data = self.get_data(self.endpoint_merge('sensor'))
        except Warning as w:
            print(f"Warning encountered: {w} !")
            self.status = {self.sensorId: "Error"}

        return data

    def get_sensor_data(
        self,
        from_date="1970-01-01T00:00:00Z",
        to_date=dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    ):
        res = pd.DataFrame()
        all_data = False
        default_params = {"from-date": from_date, "to-date": to_date, "format": "csv"}

        while not all_data:
            data = self.get_data(self.endpoint_merge('get_data'), params=default_params, format='csv')
            res = pd.concat([res, data], ignore_index=True)
            if len(data) > 0:
                last_date = data['createdAt'].iloc[-1]
                default_params ={'from-date': from_date,
                                 'to-date': last_date,
                                 'format': 'csv'}
            else:
                return(res.rename(columns={'value': self.metadata['title']}))
            if len(data) < 1000:
                all_data = True

        data_return = res.rename(columns={'value': self.metadata['title']})
        # data['createdAt'] = pd.to_datetime(data['createdAt'])
        return data_return
