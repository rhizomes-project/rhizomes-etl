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

def get_test_data_paths(format_, num_calls):
    "Returns list of possible paths to test data files."

    test_info = ETLEnv.instance().get_test_info()

    data_path = f"etl/tests/data/{test_info.institution}"
    if test_info.tag:

        data_path += f"_{test_info.tag}"

    return [ data_path + f".{format_}", data_path + f"_{num_calls}.{format_}" ]

def try_to_read_test_data(num_calls=0):
    "Check if there is sample data in a file to use for testing - if found, return the contents of the file."

    for format_ in [ "json", "xml" ]:

        data_paths = get_test_data_paths(format_=format_, num_calls=num_calls)

        for data_path in data_paths:

            if os.path.exists(data_path):

                with open(data_path, "r") as input:

                    if format_ == "json":

                        data = json.loads(input.read())

                    else:

                        data = input.read()

                    return data, format_

    return None, None


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

        self.num_calls = 0 if self.num_calls is None else self.num_calls + 1

        data, format_ = try_to_read_test_data(num_calls=self.num_calls)
        if data:

            if format_ == "json":

                return MockResponse(data=data)

            else:

                return MockResponse(content=data)

        # No test data exists for this institutions: Just do the actual http get.
        data = REAL_GET(url=url, params=params, **kwargs)

        if True:

            data_paths = get_test_data_paths(format_="txt", num_calls=self.num_calls)

            with open(data_paths[0], "w") as output:

                output.write(data.text)

            print(f"Wrote data to test data file '{data_paths[0]}'", file=sys.stderr)

        return data


class TestBase(unittest.TestCase):

    def setUp(self):

        self.maxDiff = None
        self.debug = True
        self.inspect_output = True

        etl_env = ETLEnv.instance()
        etl_env.init_testing()
        etl_env.set_use_cache(use_cached_metadata=True)

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
