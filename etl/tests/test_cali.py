from io import StringIO
import sys

import unittest

from etl.tests.test_tools import TestBase


class TestCali(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Image\r\n36702f2b0f4f2e5383feddf65d894de5,"California Development Company headquarters in Calexico, ca.1910",,"Photograph of people standing in front of the headquarters of California Development Company in Calexico, ca.1910. Three people, two men and a woman, lean against a fence on the far side of the dirt road in the foreground. Behind the fence stands a large building with a yard decorated with small trees. More fenced-in properties can be seen down the street in the right background.",circa 1910,,image,,http://doi.org/10.25549/chs-m7282,"california historical society collection 18601960:California Historical Society Collection, 1860-1960:https://registry.cdlib.org/api/v1/collection/27087/",,,Dwellings | Buildings | Real estate business | Imperial County--Calexico,,,,,,https://calisphere.org/clip/500x500/d52954842f95806e6f64fdc78ba5a31e\r\n\n'

        self.run_etl_test(institution="cali", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main()
