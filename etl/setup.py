#!/usr/bin/env python

"""
Perform any startup tasks to initialize ETL processes.
"""

import os
import json


SECRETS_PATH = "etl/secrets.json"


class ETLEnv(object):

    def start(self):

        with open(SECRETS_PATH) as file:
            self.secrets = json.loads(file.read())

    def get_api_key(self, name):

        return self.secrets["apis"]["keys"][name]
