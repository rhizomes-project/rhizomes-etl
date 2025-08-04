#!/usr/bin/env python

from datetime import date
import sys

from backup import do_backup
from run import INST_ETL_MAP


if __name__ == "__main__":    # pragma: no cover

    institutions = list(INST_ETL_MAP.keys())

    for institution in institutions:

        file_name = f"{date.today().strftime('%Y%m%d')}_{institution}.csv"
        with open(file_name, "w") as output_file:

            do_backup(institution=institution, output_file=output_file)

    sys.exit(0)
