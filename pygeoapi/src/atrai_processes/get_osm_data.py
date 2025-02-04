import os
import logging
from opensensemaptoolbox import OpenSenseMap
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


LOGGER = logging.getLogger(__name__)

METADATA = {
    'version': '0.2.0',
    'id': 'get_osm_data',
    'title': {
        'en': 'get_osm_data',
    },
    'description': {
        'en': 'downloads osm data as csv by given osm box id'},
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
        'ids': {
            'title': 'boxids',
            'description': 'boxids to get the data from',
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

class OSMDataCollector(BaseProcessor):
    def __init__(self, processor_def):

        super().__init__(processor_def, METADATA)
        self.base_data_dir = '/data'
        self.secret_token = os.environ.get('INT_API_TOKEN', 'token')

    def execute(self, data):
        mimetype = 'application/json'
        #get parameters from {$HOST}/processes/get-osm-data/execution endpoint
        self.boxIds = data.get('ids')
        self.token = data.get('token')

        #check if inputs are sufficient
        if self.boxIds is None:
            raise ProcessorExecuteError('Cannot process without a ids')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if self.token != self.secret_token:
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')


        #do the thing
        OSM = OpenSenseMap.OpenSenseMap()
        OSM.add_box(self.boxIds)
        OSM.save_OSM()


        outputs = {
            'id': 'get_osm_data',
            'status': f'downloaded data for {self.boxIds}'
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<OSMDataCollector> {self.name}'