#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestICAA(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\n1468559,ICAA Working Papers Number 2,,,2020-02-14T02:06:44+00:00,,,,https://icaa.mfah.org/s/en/item/1468559,,,,,,,,,,\r\n\n'

        self.run_etl_test(institution="icaa", format="csv", expected=expected)

if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestICAA())
