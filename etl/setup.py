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

    def init_testing(self):
        "Set system up for testing."

        os.environ["PYTHONPATH"] = os.getcwd()
        os.environ["RUNNING_UNITTESTS"] = "1"

        self.running_tests = True

    def are_tests_running(self):
        "Returns True if tests are runnning."

        return self.running_tests
