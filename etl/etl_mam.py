#!/usr/bin/env python

import re
import requests
import sys

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import get_date_first_four

import openpyxl


field_map = {

    "ACCESSNO":                             RhizomeField.ID,
    "TITLE":                                RhizomeField.TITLE,
    "CREATOR":                              RhizomeField.AUTHOR_ARTIST,
    "OBJECT URL":                           RhizomeField.URL,
    "DESCRIP":                              RhizomeField.DESCRIPTION,
    "STERMS":                               RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "DATE":                                 RhizomeField.DATE,
    "COLLECTION":                           RhizomeField.SOURCE,
    "OBJECT IMAGE":                         RhizomeField.IMAGES,

    "CREATOR2":                             RhizomeField.AUTHOR_ARTIST,

}


field_map_keys = list(field_map.keys())


# Map column name to column index in the input
# data (in case the column order changes)
column_indices = {

    # ACCESSNO
    field_map_keys[0]: 0,

    # TITLE
    field_map_keys[1]: 16,

    # CREATOR
    field_map_keys[2]: 3,

    # OBJECT URL
    field_map_keys[3]: 22,

    # DESCRIP
    field_map_keys[4]: 6,

    # STERMS
    field_map_keys[5]: 14,

    # DATE
    field_map_keys[6]: 5,

    # COLLECTION
    field_map_keys[7]: 1,

    # OBJECT IMAGE
    field_map_keys[8]: 23,

    # CREATOR2
    field_map_keys[9]: 4,

}



required_values = [
    "ACCESSNO",
    "TITLE",
    "DESCRIP",
    "OBJECT IMAGE"
]

bad_strings = [
    "\n"
]


def is_record_valid(record):

    for required_value in required_values:

        if not record.get(required_value):

            message = f"Missing {required_value}"

            return False, message

    return True, None

def clean_value(value):

    # Is this value on a blank line?
    if not value:

        return None

    # This function can only clean strings.
    if type(value) is not str:

        return value

    for bad_string in bad_strings:

        value = value.replace(bad_string, " ")

    return value.strip()

def remove_parens(value):
    # Remove parenthesis from beginning and end of value.

    # REVIEW: remove this?

    if not value:

        return None

    value = value.strip()

    if value.startswith("("):

        value = value[ 1 : ]

    if value.endswith(")"):

        value = value[ : -1 ]

    return clean_value(value=value)

def parse_subject(value):

    # REVIEW: remove this?

    if not value:

        return None

    delimiters = [ ",", ";" ]

    for delimiter in delimiters:

        value = value.replace(delimiter, "|")

    bad_strings = [ " |", "| " ]
    for bad_string in bad_strings:

        value = value.replace(bad_string, "|")

    return {
        "themes": clean_value(value=value)
    }


class SpecialValueHandler():

    def __init__(self):

        self.reset()

    def reset(self):

        self.creator = ""

    def add_creator(self, value):

        if not value:

            return

        if self.creator:

            self.creator += ", "

        self.creator += value


special_value_handler = SpecialValueHandler()


def parse_values(field_name, value):

    if field_name.startswith("CREATOR"):

        special_value_handler.add_creator(value=value)

        return { "CREATOR" : special_value_handler.creator }

    else:

        return { field_name: clean_value(value=value) }


class MAMETLProcess(BaseETLProcess):

    def init_testing(self):

        pass

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "Mexic-Arte Museum (MAM)"

    def get_date_parsers(self):

        return {
            r'^\d{4}':  get_date_first_four
        }

    def extract(self):

        # Open the excel workbook.
        wrkbk = openpyxl.load_workbook("etl/data/permanent/mam/input_20240601.xlsx")
        sheet = wrkbk.active

        data = []

        # Iterate through the sheet by row & column and get the data.
        # Note: row and col numbers in openpyxl are 1-based, and we skip header the row.
        for row_num in range(1, sheet.max_row + 1):

            record = {}
            special_value_handler.reset()

            # Build each row.
            for field_name, col_index in column_indices.items():

                cell_obj = sheet.cell(row=row_num + 1, column=col_index + 1)

                values = parse_values(field_name=field_name, value=cell_obj.value)
                if values:

                    record |= values

            # Skip any blank lines.
            is_valid, message = is_record_valid(record=record)
            if is_valid:

                data.append(record)

            else:

                print(f"Row {row_num + 1} is invalid: {message}", file=sys.stderr)

        return data


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "mam" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
