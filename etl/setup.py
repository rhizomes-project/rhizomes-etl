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
            secrets = json.loads(file.read())

        self.dpla_api_key = secrets["apis"]["keys"]["dpla"]
        self.smithsonian_api_key = secrets["apis"]["keys"]["smithsonian"]
        self.calisphere_api_key = secrets["apis"]["keys"]["calisphere"]
