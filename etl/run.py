#!/usr/bin/env python


import sys

from etl import setup
from etl.etl_calisphere import CalisphereETLProcess
from etl.etl_dpla import DPLAETLProcess
from etl.etl_pth import PTHETLProcess
from etl.etl_si import SIETLProcess


# REVIEW: look into date issues:
#   - for calisphere, date is treated as a display date ('date') a sort date ('date_ss') and then sort_date_start and sort_date_end
#   - for dpla, date is treated as 'displaydate' and then 'begin' and 'end', and is sometimes not available
#   - for SI date is frequently more of a subject or topic, possibly with multiple values, and sometimes not available
#   - figure out how to deal with dates - separate search field and display field?

# REVIEW: TODO put internet archive into new ETL layout (first pass at least)


INST_ETL_MAP = {
    "cali": CalisphereETLProcess,
    "dpla": DPLAETLProcess,
    "pth": PTHETLProcess,
    "si": SIETLProcess,
}


def run_etl(institutions, format):

    etl_classes = [ INST_ETL_MAP[inst] for inst in institutions ]

    for etl_class in etl_classes:

        etl_process = etl_class(format=format)

        data = etl_process.extract()
        etl_process.transform(data=data)
        etl_process.load(data=data)

def do_usage(msg=None):
    "Output usage exception."

    if msg:

        print(msg, file=sys.stderr)

    print("Usage: run.py institution1 ... institutionN --format[=csv] --use_cache=[yes|no] --resume_download=[offset] --category", file=sys.stderr)

    raise Exception("Invalid usage")

def run_cmd_line(args):

    format_ = "csv" # default output format.
    use_cache = "no"
    institutions = []

    # Parse command-line args.
    for idx, arg in enumerate(args):

        if arg.startswith("--format="):

            if len(arg) < 12:

                do_usage(msg=f"Invalid format: {arg}")

            pos = arg.find('=')
            format_ = arg[ pos + 1 : ]

        elif arg.startswith("--use_cache="):

            if len(arg) not in [ 14, 15 ]:

                raise Exception(f"Invalid format: {arg}")

            pos = arg.find('=')
            use_cache = arg[ pos + 1 : ]

            setup.ETLEnv.instance().set_use_cache(use_cached_metadata=(use_cache == "yes"))

        elif arg.startswith("--resume_download="):

            if len(arg) < 19:

                raise Exception(f"Invalid offset value: {arg}")

            pos = arg.find('=')
            offset = arg[ pos + 1 : ]

            setup.ETLEnv.instance().set_call_offset(offset=int(offset))

        elif arg.startswith("--category"):

            if len(arg) < 11:

                raise Exception(f"Invalid category value: {arg}")

            pos = arg.find('=')
            category = arg[ pos + 1 : ]

            setup.ETLEnv.instance().set_category(category=category)

        else:

            if arg not in INST_ETL_MAP:

                do_usage(msg=f"Invalid institution: {arg}")

            institutions.append(arg)

    if not institutions:

        do_usage()

    # Run the ETL.
    run_etl(institutions=institutions, format=format_)


if __name__ == "__main__":    # pragma: no cover

    run_cmd_line(sys.argv[ 1: ])
