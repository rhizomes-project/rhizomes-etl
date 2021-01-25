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
current_tag = None


class MockResponse():

    def __init__(self, data):

        self.data = data

    def json(self):

        return self.data


class MockGetter():

    def __call__(self, url, params=None, **kwargs):

        data_path = f"etl/tests/data/{current_institution}"
        if current_tag:

            data_path = f"_{current_tag}"

        data_path +=  ".json"

        if path.exists(data_path):

            with open(data_path, "r") as input:

                return MockResponse(data=json.loads(input.read()))

        else:

            return REAL_GET(url=url, params=params, **kwargs)


class TestBase(unittest.TestCase):

    def setUp(self):

        self.maxDiff = None
        self.debug = False

    def run_etl_test(self, institution, format, expected, tag=None):

        if tag:

            print(f"\nTesting ETL process for {institution} {tag}, format={format}\n", file=sys.stderr)

        else:

            print(f"\nTesting ETL process for {institution}, format={format}\n", file=sys.stderr)

        global current_institution
        current_institution = institution
        global current_tag
        current_tag = tag

        if not self.debug:

            old_stdout = sys.stdout
            mystdout = StringIO()
            sys.stdout = mystdout

        with patch.object(requests, 'get', new_callable=MockGetter):

            run_cmd_line(args=[institution]+[ "--format="+format ])

        if not self.debug:

            sys.stdout = old_stdout

            mystdout.seek(0)
            output = mystdout.read()

            # self.assertEqual(output, expected)
            self.assertEqual(len(output), len(expected))
