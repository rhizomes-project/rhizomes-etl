#!/usr/bin/env python

"""
Perform any startup tasks to initialize ETL processes.
"""

import os
import json


SECRETS_PATH = "etl/secrets.json"


class ETLEnv(object):

    # The one and only instance of the ETLEnv class.
    etl_env = None

    def __init__(self):

        # Check singleton usage.
        if ETLEnv.etl_env:

            raise Exception("ETLEnv should only be accessed via ETLEnv.instance()")

        self.running_tests = False

    @staticmethod
    def instance():
        "Returns the one and only instance of ETLEnv."

        if not ETLEnv.etl_env:

            ETLEnv.etl_env = ETLEnv()

        return ETLEnv.etl_env

    def start(self):

        with open(SECRETS_PATH) as file:
            self.secrets = json.loads(file.read())

    def get_api_key(self, name):

        return self.secrets["apis"]["keys"][name]

    def set_use_cache(self, use_cached_metadata):
        "Sets flag indicating if we should use cached metadata files."

        self.use_cached_metadata = use_cached_metadata

    def use_cache(self):
        "Returns True if we should use cached metadata files."

        return self.use_cached_metadata

    def init_testing(self):
        "Set system up for testing."

        os.environ["PYTHONPATH"] = os.getcwd()
        os.environ["RUNNING_UNITTESTS"] = "1"

        self.running_tests = True

    def are_tests_running(self):
        "Returns True if tests are runnning."

        return self.running_tests

    class TestInfo():

        def __init__(self, institution, tag=''):

            self.institution = institution
            self.tag = tag

    def get_test_info(self):
        "Returns test config info."

        return self.test_info

    def set_test_info(self, test_info):
        "Sets test config info."

        self.test_info = test_info
