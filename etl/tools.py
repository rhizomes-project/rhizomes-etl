#!/usr/bin/env python

import csv
import json
import sys


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

                    if format == "csv":

                        buf += tmp + "\n"

                    else:

                        buf += "\n\t" + tmp

            if format == "csv":

                buf = buf.strip()

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

        elif self.format == "csv":

            # REVIEW: TODO put these fieldnames in one place only
            fieldnames = [ "Title", "Author/Artist", "Description", "Date", "Digital Format", "Resource Identifier", "Source", "Subjects (Topic/Keywords)", ]

            self.output = csv.DictWriter(sys.stdout, fieldnames=fieldnames, dialect=csv.QUOTE_ALL)

            self.row_buf = {}

        else:

            self.output = ""

    def start_collection(self):

        if self.format == "csv":

            self.output.writeheader()

        else:

            pass

    def start_record(self):

        if self.format == "json":

            self.output.append({})

        if self.format == "csv":

            self.row_buf = {}

        else:

            self.output += "\n"

    def add_value(self, name, value):

        value = get_value(value=value, format=self.format)
        if self.format == "json":

            pos = len(self.output) - 1
            self.output[pos][name] = value

        elif self.format == "csv":

            self.row_buf[name] = value

        else:

            self.output += f"{name}: {value}\n"

    def end_record(self):

        if self.format == "csv":

            self.output.writerow(self.row_buf)

        else:

            pass

    def end_collection(self):

        if self.format == "json":

            buf = json.dumps(self.output)

        elif self.format == "csv":

            buf = ""

        else:

            buf = self.output

        print(buf)
