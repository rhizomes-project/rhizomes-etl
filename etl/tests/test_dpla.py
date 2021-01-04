from io import StringIO
import sys

import unittest

from etl.tests.test_tools import TestBase


class TestDPLA(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line\r\n2af161a022fa94eaa406bf7bb9e859f6,Chicano Park: Chicano Pinto Union,"Vargas, Tony de (American muralist, active 20th century)","Paintings | JUSTICIA PARA LAS PINTOS (justice for the inmates) | Digital Library Development Program, UC San Diego, La Jolla, 92093-0175 (https://lib.ucsd.edu/digital-library) | Chicano Park (San Diego, California) | Barrio Logan (San Diego, California)",1978,image,,,https://library.ucsd.edu/dc/object/bb73413757,Cinewest Archive,No linguistic content; Not applicable,,Parks | Mural painting and decoration | Symbolism | Mexican American art | Bridges | History | California,California,,,"UC San Diego, Library, Digital Library Development Program",\r\n\n'

        self.run_etl_test(institution="dpla", format="csv", expected=expected)


if __name__ == '__main__':

    unittest.main()
