#!/usr/bin/env python

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField


# REVIEW: TODO: update this for NMAA
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

    def extract(self):

        # REVIEW: TODO: add extraction


        return data


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "nmma" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
