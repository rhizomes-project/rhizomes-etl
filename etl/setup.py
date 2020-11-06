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


        # REVIEW TODO get rid of these
        self.dpla_api_key = self.secrets["apis"]["keys"]["dpla"]
        self.smithsonian_api_key = self.secrets["apis"]["keys"]["smithsonian"]
        self.calisphere_api_key = self.secrets["apis"]["keys"]["calisphere"]

    def get_api_key(self, name):

        return self.secrets["apis"]["keys"][name]
