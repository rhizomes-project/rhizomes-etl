#!/usr/bin/env python


import requests
import json

from setup import ETLEnv


protocol = "https://"
domain = "api.si.edu"
etl_env = ETLEnv()
etl_env.start()
api_key = etl_env.smithsonian_api_key

query_path = "/openaccess/api/v1.0/search"
query_url = protocol + domain + query_path + "?api_key=" + api_key


keys_to_ignore = ("title_sort", "type", "label")
keys_to_not_label = ("content", )


def traverse(data, key=None, indents=0):

    if key and key in keys_to_ignore:

        return

    if type(data) is dict:

        for key in data.keys():

            if data.get(key, None):

                traverse(data=data[key], key=key, indents=indents)

    elif type(data) is list:

        if key:

            print(key + ":")

        for val in data:

            traverse(data=val, indents=indents+1)

    else:

        if key in keys_to_not_label:

            label = "- "

        else:

            label = key + ": " if key else ""

        sep = "    "
        print(f"{indents * sep}{label}{data}")


if __name__ == "__main__":

    search_terms = [ "chicano" ]
    for search_term in search_terms:

        response = requests.get(query_url + "&q=" + search_term)

        data = response.json()

        for row in data["response"]["rows"]:

            traverse(data=row)

            print("\n")
