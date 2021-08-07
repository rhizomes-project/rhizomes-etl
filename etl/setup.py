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
        self.rebuild_previous_items = False
        self.use_cached_metadata = False
        self.offset = None
        self.dupes_file = None
        self.category = None

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

    def set_rebuild_previous_items(self, rebuild_previous_items):
        "Sets flag indicating if we should ignore items that are already loaded in the website."

        self.rebuild_previous_items = rebuild_previous_items

    def do_rebuild_previous_items(self):

        return self.rebuild_previous_items

    def set_use_cache(self, use_cached_metadata):
        "Sets flag indicating if we should use cached metadata files."

        self.use_cached_metadata = use_cached_metadata

    def use_cache(self):
        "Returns True if we should use cached metadata files."

        return self.use_cached_metadata

    def set_call_offset(self, offset):

        self.offset = offset

    def get_call_offset(self):

        return self.offset

    def set_dupes_file(self, dupes_file):

        self.dupes_file = dupes_file

    def get_dupes_file(self):

        return self.dupes_file

    def set_category(self, category):

        self.category = category

    def get_category(self):

        return self.category

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
