#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestPTH(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth304825 | info:ark/67531/metapth304825 | local-cont-no: MAMU_2004-041,[Speaker at Gallery Talk],"Mexic-Arte Museum (Austin, Tex.)","Photograph of a gallery talk at Mexican Report: Contemporary Art from Mexico, an exhibition hosted by the Mexic-Arte Museum in Austin, Texas. The speaker stands behind a podium. He holds a microphone. A painting hangs on the wall behind him. The painting features a pattern of gray squares.",2004-09-12,Photograph,Image,1 photograph : col. ; 4 x 6 in.,https://texashistory.unt.edu/ark:/67531/metapth304825/,"Mexican Report, Mexic-Arte Museum, Austin, Texas, 2004",No Language,"2004-09-12 | Into Modern Times, 1939-Present",Arts and Crafts | Museum exhibits -- Texas -- Austin. | Social Life and Customs - Fairs and Exhibitions | contemporary art,United States - Texas - Travis County - Austin,,,,,https://texashistory.unt.edu/ark:/67531/metapth304825/thumbnail\r\nark: ark:/67531/metapth836648 | info:ark/67531/metapth836648 | local-cont-no: Briones_Francisco-2015-06-25,"Oral History Interview with Francisco Briones, June 25, 2015","Briones, Francisco | Enriquez, Sandra | Robles, David","Interview with Francisco Briones, a civil rights activist from Alamo, Texas, near McAllen. In his interview, Briones discusses his early life, his education at Colegio Jacinto Treviño, and his activism in the Chicano movement in South Texas.",2015-06-25,Video,,"3 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836648/,https://crbb.tcu.edu/interviews/interview-with-francisco-briones,English,,"Briones, Francisco | People - Ethnic Groups | People - Individuals | biographies | interviews | oral histories",United States - Texas - Hidalgo County - Alamo,,,,,https://texashistory.unt.edu/ark:/67531/metapth836648/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestPTH())
