#!/usr/bin/env python

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
    "creator_accessionPart_full_name":         RhizomeField.AUTHOR_ARTIST,
    "media":                                   RhizomeField.RESOURCE_TYPE,
    "a17dimensions":                           RhizomeField.DIMENSIONS,
    "creation_year":                           RhizomeField.DATE,
}


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

            row = {}

            # Build each row.
            for col_num, field_name in enumerate(field_map.keys()):

                cell_obj = sheet.cell(row=row_num + 1, column=col_num + 1)
                row[field_name] = cell_obj.value

            data.append(row)

        return data


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "nmma" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
