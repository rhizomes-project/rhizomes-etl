#!/usr/bin/env python

import json


import pdb


# TODO: remove all newlines?


def pretty_print(name, value):

    pass


def get_value(value, format="json"):

    if type(value) is list:

        if len(value) == 1:

            return value[0]

        else:

            if format == 'json':

                return value

            buf = ""
            for tmp in value:

                if type(tmp) is not str:


                    pdb.set_trace()


                else:

                    buf += "\n\t" + tmp

            return buf

    elif type(value) is dict:


        pdb.set_trace()

        return ""


    else:

        return value


class MetadataWriter():

    def __init__(self, format):

        self.format = format
        if self.format == "json":

            self.output = []

        else:

            self.output = ""

    def start_collection(self):

        pass

    def start_record(self):

        if self.format == "json":

            self.output.append({})

        else:

            self.output += "\n"

    def add_value(self, name, value):

        value = get_value(value=value, format=self.format)
        if self.format == "json":

            pos = len(self.output) - 1
            self.output[pos][name] = value

        else:

            self.output += f"{name}: {value}\n"

    def end_record(self):

        pass

    def end_collection(self):

        if self.format == "json":

            buf = json.dumps(self.output)

        else:

            buf = self.output

        print(buf)
