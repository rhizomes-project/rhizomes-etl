from io import StringIO
import json
from os import path
import requests
import sys

import unittest
from unittest.mock import patch

from etl.run import run_cmd_line


REAL_GET = requests.get
current_institution = None


class MockResponse():

    def __init__(self, data):

        self.data = data

    def json(self):

        return self.data


class MockGetter():

    def __call__(self, url, params=None, **kwargs):

        data_path = f"etl/tests/data/{current_institution}.json"
        if path.exists(data_path):

            with open(data_path, "r") as input:

                return MockResponse(data=json.loads(input.read()))

        else:

            return REAL_GET(url=url, params=params, **kwargs)


class MockedResponse():

    def __init__(self, response_ok=True, data={}):
        self.ok = response_ok
        self.data = data

    def json(self):
        return self.data


class TestBase(unittest.TestCase):

    def setUp(self):

        self.maxDiff = None

    def run_etl_test(self, institution, format, expected):

        print(f"\nTesting ETL process for {institution}, format={format}\n", file=sys.stderr)

        global current_institution
        current_institution = institution

        old_stdout = sys.stdout
        mystdout = StringIO()
        sys.stdout = mystdout

        with patch.object(requests, 'get', new_callable=MockGetter):

            run_cmd_line(args=[institution]+[ "--format="+format ])

        sys.stdout = old_stdout

        mystdout.seek(0)
        output = mystdout.read()

        self.assertEqual(output, expected)
