#!/usr/bin/env python

from datetime import date
import sys

from backup import do_backup, get_institution_names, run_institution_backup_cli
from run import INST_ETL_MAP


if __name__ == "__main__":    # pragma: no cover

    institution = sys.argv[ 1: ][0] if len(sys.argv) == 2 else None

    # Back up a single institution?
    if institution:

        run_institution_backup_cli(institution=institution)

    else:

        # Back up all institutions, each into its own csv file.
        institutions = get_institution_names()

        for institution in institutions:

            file_name = f"{date.today().strftime('%Y%m%d')}_{institution}.csv"
            with open(file_name, "w") as output_file:

                do_backup(institution=institution, output_file=output_file)

        sys.exit(0)
