from io import StringIO
import sys

import unittest

from etl.run import run_cmd_line


class TestBase(unittest.TestCase):

    def setUp(self):

        self.maxDiff = None

    def run_etl_test(self, institution, format, expected):

        print(f"\nTesting ETL process for {institution}, format={format}\n", file=sys.stderr)

        if type(institution) is list:

            institutions = institution

        else:

            institutions = [ institution ]

        old_stdout = sys.stdout
        mystdout = StringIO()
        sys.stdout = mystdout

        run_cmd_line(args=institutions+[ "--format="+format ])

        sys.stdout = old_stdout

        mystdout.seek(0)
        output = mystdout.read()

        self.assertEqual(output, expected)
