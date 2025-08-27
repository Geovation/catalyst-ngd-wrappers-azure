import os
from unittest import TestCase
import requests as r
from dotenv import load_dotenv
load_dotenv()

ROOT_URL = os.environ.get('ROOT_URL', '')
KEY = os.environ.get('CLIENT_ID', '')
GLOBAL_TIMEOUT = 20

class NGDTestCase(TestCase):
    '''Base class for NGD wrapper API tests.'''

    def test_filter_combos(self):
        '''Test for the handling of a combination of "WKT" and "filter"'''
        endpoint = ROOT_URL + 'catalyst/features/bld-fts-building-4/items'
        response = r.get(
            endpoint,
            params = {
                'wkt': '''POLYGON ((-0.10219 51.52429, -0.10192 51.52389, -0.10095 51.52407, -0.10169 51.52463, -0.10219 51.52429))''',
                'filter': "buildinguse_oslandusetiera IN ('Residential Accommodation','Commercial Activity: Other')",
                'log-request-details': True
            },
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 200, response.text)

    def test_invalid_query_params(self):
        """
        Test for invalid query parameters in the NGD API.
        This function sends a request with an unsupported query parameter and checks the response.
        It expects a 400 status code and specific error messages in the response.
        """
        endpoint = ROOT_URL + 'catalyst/features/lnd-fts-land-1/items'
        response = r.get(
            endpoint,
            params={'test': 'should-fail'},
            headers = {'key': KEY},
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 400, response.text)
        json_response = response.json()
        keys = json_response.keys()
        self.assertIn('description', keys)
        self.assertIn('errorSource', keys)
        startswith_text = 'Not supported query parameter(s): test. Supported NGD parameters are:'
        self.assertTrue(json_response.get('description', '').startswith(startswith_text))
        self.assertEqual(json_response.get('errorSource', ''), 'OS NGD API')

    def test_hiearchical_request(self):
        ''' Test for a hierarchical request to the NGD API.
        A more complex request making use of various features.
        Various checks that the response is in the expected format.'''
        wkt = '''
        GEOMETRYCOLLECTION(
            MULTIPOLYGON(
                ((558288 104518, 558288 104528, 558298 104528, 558298 104518, 558288 104518)),
                ((558388 104318, 558388 104328, 558398 104328, 558398 104318, 558388 104318))
            ),
            LINESTRING(
                558288 104518, 558298 104528, 558398 104328, 558398 104318
            )
        )'''
        endpoint = ROOT_URL + 'catalyst/features/multi-collection/items/geom-col'
        response = r.get(
            endpoint,
            params = {
                'wkt': wkt,
                'filter-crs': 27700,
                'crs': 3857,
                'collection': 'lnd-fts-land,bld-fts-building,wtr-fts-water',
                'hierarchical-output': True,
                'use-latest-collection': True
            },
            headers = {
                'erroneous-header': 'should-be-ignored',
                'key': KEY
            },
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 200, response.text)
        json_response = response.json()
        response_keys = list(json_response.keys())
        self.assertEqual(len(response_keys), 3)
        self.assertTrue(response_keys[0].startswith('lnd-fts-land-'))
        feature_keys = list(
            json_response \
            .get('lnd-fts-land-3',{}) \
            .get('searchAreas',{})[0] \
            .keys()
        )
        self.assertListEqual(feature_keys, [
            'type',
            'links',
            'timeStamp',
            'numberReturned',
            'features',
            'code',
            'numberOfRequests',
            'telemetryData',
            'searchAreaNumber'
        ])

    def test_flat_request(self):
        """Test for a non-hierarchical request to the NGD API.
        Various checks that the response is in the expected format.
        Includes handling of a mixture of version-specified and non-versioned collections with 'use-latest-collection'"""
        endpoint = ROOT_URL + 'catalyst/features/multi-collection/items/limit-col'
        response = r.get(
            endpoint,
            params = {
                'crs': 'http://www.opengis.net/def/crs/EPSG/0/27700',
                'collection': [
                    'gnm-fts-crowdsourcednamepoint',
                    'bld-fts-building-1',
                    'trn-ntwk-pathlink'
                ],
                'use-latest-collection': True,
                'limit': 213,
                'key': KEY,
                'authenticate': False
            },
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 200, response.text)
        json_response = response.json()
        response_keys = list(json_response.keys())
        self.assertListEqual(response_keys, [
            'type',
            'numberOfRequests',
            'numberOfRequestsByCollection',
            'numberReturned',
            'numberReturnedByCollection',
            'features',
            'timeStamp'
        ])
        number_returned_by_collection = json_response.get('numberReturnedByCollection', {})
        self.assertIn('bld-fts-building-1', number_returned_by_collection)
        self.assertEqual(len(number_returned_by_collection), 3)
        self.assertTrue(all(v == 213 for v in number_returned_by_collection.values()))
        self.assertEqual(json_response.get('numberReturned', 0), 639)
        number_of_requests_by_collection = json_response.get('numberOfRequestsByCollection', {})
        self.assertEqual(len(number_of_requests_by_collection), 3)
        self.assertTrue(all(v == 3 for v in number_of_requests_by_collection.values()))
        self.assertEqual(json_response.get('numberOfRequests', 0), 9)
        first_feature_keys = list(json_response.get('features', [])[0].keys())
        self.assertListEqual(first_feature_keys, [
            'id',
            'type',
            'geometry',
            'properties',
            'collection'
        ])

    def test_invalid_key(self):
        '''
        Test for invalid API key in the NGD API.
        This function sends a request with an invalid key and checks the response.
        It expects a 401 status code and specific error messages in the response.
        '''
        endpoint = ROOT_URL + 'catalyst/features/lnd-fts-land-1/items'
        response = r.get(
            endpoint,
            params = {'authenticate': 'false'},
            headers = {'key': 'invalid-key'},
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 401, response.text)
        json_response = response.json()
        self.assertDictEqual(
            json_response,
            {
                "code": 401,
                "description": "Missing or unsupported API key provided.",
                "errorSource": "OS NGD API"
            }
        )

    def test_latest_collections_single(self):
        ''' Test for retrieving the latest collection for a specific collection type.
        Tests that the response is in the expected format'''

        collection = 'lnd-fts-land'
        endpoint = f'{ROOT_URL}catalyst/latest-collections/{collection}'
        response = r.get(
            endpoint,
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 200, response.text)
        json_response = response.json()
        val = json_response.get(collection)
        self.assertIsNotNone(val)
        self.assertTrue(val.startswith(collection))
        self.assertFalse(val.endswith('-1'))  # Should not be the first collection

    def test_latest_collections(self):
        '''Test for retrieving the latest collections.
        Tests that the response is in the expected format and contains the expected keys.'''

        endpoint = ROOT_URL + 'catalyst/latest-collections'
        response = r.get(
            endpoint,
            params = {'recent-update-days': 28},
            headers = {'key': KEY}, # Isn't necessary, but should be ignored
            timeout = GLOBAL_TIMEOUT
        )
        self.assertEqual(response.status_code, 200, response.text)
        json_response = response.json()
        first_key = list(json_response)
        self.assertListEqual(first_key, [
            'collection-lookup',
            'recent-update-threshold-days',
            'recent-collection-updates'
        ])
