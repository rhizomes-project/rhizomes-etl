from io import StringIO
import sys

import unittest

from etl.tests.test_tools import TestBase


class TestDPLA(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line\r\ninfo:ark/67531/metapth304760 | local-cont-no: MAMU_2003-040 | ark: ark:/67531/metapth304760,[Postcard: The 20th Annual Día de los Muertos Family],,"Postcard that advertises ""The 20th Annual Día de los Muertos Family Event"" Celebration, which was presented by the Official Mexican and Mexican American Fine Art Museum of Texas and the Mexic-Arte Museum. The front of the photograph features a black and white photograph of an outdoor Mexican altar with photo credits included at the bottom of the image. On the right-hand side of the postcard, there is a list of events that will be at the celebration as well as the date of the event, the logo of the Mexic-Arte Museum, and museum contact information. The flip side of the postcard features a more detailed itinerary of the major events that will be at the celebration. There is also sponsor and admission information.",2003,Postcard,Image,1 postcard : col. ; 14 cm.,https://texashistory.unt.edu/ark:/67531/metapth304760/,"Día de los Muertos, Mexic-Arte Museum, Austin, Texas, November 2003",English,"Into Modern Times, 1939-Present | 2003-11-01",All Souls\' Day -- Texas -- Austin. | Museum exhibits -- Texas -- Austin. | Social Life and Customs - Customs - Celebrations | People - Family Groups | People - Ethnic Groups - Hispanics | Social Life and Customs - Customs - Holidays | Día de los Muertos | Day of the Dead | events | presentations | altars | communities,United States - Texas - Travis County - Austin,,,,\r\n\n'

        self.run_etl_test(institution="pth", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main()
