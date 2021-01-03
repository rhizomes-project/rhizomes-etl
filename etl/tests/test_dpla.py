from io import StringIO
import sys

import unittest

from etl.run import run_etl


class TestDPLA(unittest.TestCase):

    def test(self):

        old_stdout = sys.stdout
        mystdout = StringIO()
        sys.stdout = mystdout

        run_etl(institutions=["dpla"], format="csv")

        sys.stdout = old_stdout

        mystdout.seek(0)
        output = mystdout.read()

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line\r\n2af161a022fa94eaa406bf7bb9e859f6,Chicano Park: Chicano Pinto Union,"Vargas, Tony de (American muralist, active 20th century)","Paintings | JUSTICIA PARA LAS PINTOS (justice for the inmates) | Digital Library Development Program, UC San Diego, La Jolla, 92093-0175 (https://lib.ucsd.edu/digital-library) | Chicano Park (San Diego, California) | Barrio Logan (San Diego, California)",1978,image,,,https://library.ucsd.edu/dc/object/bb73413757,Cinewest Archive,No linguistic content; Not applicable,,Parks | Mural painting and decoration | Symbolism | Mexican American art | Bridges | History | California,California,,,"UC San Diego, Library, Digital Library Development Program",\r\n\n'

        self.assertEqual(output, expected)


if __name__ == '__main__':

    unittest.main()
