import csv
from io import StringIO
import json
import os
import requests
import sys

import unittest
from unittest.mock import patch

from etl.run import run_cmd_line
from etl.setup import ETLEnv

import pdb


REAL_GET = requests.get


class MockResponse():

    def __init__(self, data=None, content=None):

        self.data = data
        self.content = content
        self.ok = True

    def json(self):

        return self.data


class MockGetter():

    def __init__(self):

        self.num_calls = None

    def __call__(self, url, params=None, **kwargs):

        test_info = ETLEnv.instance().get_test_info()

        self.num_calls = 0 if self.num_calls is None else self.num_calls + 1

        for format_ in [ "json", "xml" ]:

            data_path = f"etl/tests/data/{test_info.institution}"
            if test_info.tag:

                data_path += f"_{test_info.tag}"

            data_paths = [ data_path + f".{format_}", data_path + f"_{self.num_calls}.{format_}" ]

            for data_path in data_paths:

                if os.path.exists(data_path):

                    with open(data_path, "r") as input:

                        if format_ == "json":

                            return MockResponse(data=json.loads(input.read()))

                        else:

                            return MockResponse(content=input.read())

        # No test data exists for this institutions: Just do the actual http get.
        data = REAL_GET(url=url, params=params, **kwargs)

        if True:

            with open(data_paths[0], "w") as output:

                output.write(data.text)

            print(f"Wrote data to test data file '{data_paths[0]}'", file=sys.stderr)

        return data


class TestBase(unittest.TestCase):

    def setUp(self):

        self.maxDiff = None
        self.debug = False
        self.inspect_output = False

        ETLEnv.instance().init_testing()

    def run_etl_test(self, institution, format, expected, tag=''):

        print(f"\nTesting ETL process for {institution} {tag}, format={format}\n", file=sys.stderr)

        test_info = ETLEnv.TestInfo(institution=institution, tag=tag)
        ETLEnv.instance().set_test_info(test_info=test_info)

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

            # Break to examine output?
            if self.inspect_output:    # pragma: no cover (this is just for debugging)

                pdb.set_trace()

            # Convert output to csv and compare the csv.
            output_csv = []
            for row in csv.reader(StringIO(output)):

                output_csv.append(row)

            expected_csv = []
            for row in csv.reader(StringIO(expected)):

                expected_csv.append(row)

            self.assertEqual(output_csv, expected_csv)
