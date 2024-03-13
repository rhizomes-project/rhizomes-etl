#!/usr/bin/env python

import requests

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import get_date_first_four

import openpyxl


field_map = {

    "accession_num":                           RhizomeField.ID,
    "title":                                   RhizomeField.TITLE,
    "translation":                             RhizomeField.DESCRIPTION,
    "web_url":                                 RhizomeField.URL,
    "image_url":                               RhizomeField.IMAGES,
    "creator_accessionPart_full_name":         RhizomeField.AUTHOR_ARTIST,
    "media":                                   RhizomeField.RESOURCE_TYPE,
    "a17dimensions":                           RhizomeField.DIMENSIONS,
    "creation_year":                           RhizomeField.DATE,
}

required_values = [
    "accession_num",
    "title",
    "translation"
]

bad_strings = [
    "\n"
]


def is_record_valid(record):

    for required_value in required_values:

        if not record.get(required_value):

            return False

    return True

def clean_value(value):

    # Is this value on a blank line?
    if not value:

        return None

    for bad_string in bad_strings:

        pos = value.find(bad_string)
        if pos >= 0:


            value = value.replace(bad_string, " ")

        return value

def extract_image_url(record):

    accession_num = record["accession_num"]
    url = record["web_url"]

    response = requests.get(url=url, timeout=60)
    content = response.text

    url_beginning = 'data-srcset="'
    url_end = ".png"

    begin = content.find(url_beginning)
    end = content.find(url_end, begin)

    url = content[ begin + len(url_beginning) : end + len(url_end) ]

    return "https://nationalmuseumofmexicanart.org" + url


class NMAAETLProcess(BaseETLProcess):

    def init_testing(self):

        pass

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "National Museum of Mexican Art (NMMA)"

    def get_date_parsers(self):

        return {
            r'^\d{4}':  get_date_first_four
        }

    def extract(self):

        # Open the excel workbook.
        wrkbk = openpyxl.load_workbook("etl/data/permanent/nmma/NMMA_Proj.Rhizome.xlsx")
        sheet = wrkbk.active

        data = []

        # Iterate through the sheet by row & column and get the data.
        # Note: row and col numbers in openpyxl are 1-based, and we skip header the row.
        for row_num in range(1, sheet.max_row + 1):

            record = {}

            # Build each row.
            for col_num, field_name in enumerate(field_map.keys()):

                cell_obj = sheet.cell(row=row_num + 1, column=col_num + 1)

                value = clean_value(value=cell_obj.value)
                record[field_name] = value

            # Skip any blank lines.
            if is_record_valid(record=record):

                # Parse the image url from the web page.
                record["image_url"] = extract_image_url(record=record)

                data.append(record)

        return data


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "nmma" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
