from io import StringIO
import sys

import unittest

from etl.run import run_etl


class TestBase(unittest.TestCase):

    def run_etl_test(self, institution, format, expected):

        if type(institution) is list:

            institutions = institution

        else:

            institutions = [ institution ]

        old_stdout = sys.stdout
        mystdout = StringIO()
        sys.stdout = mystdout

        run_etl(institutions=institutions, format=format)

        sys.stdout = old_stdout

        mystdout.seek(0)
        output = mystdout.read()

        self.assertEqual(output, expected)
