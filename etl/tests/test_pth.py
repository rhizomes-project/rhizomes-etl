#!/usr/bin/env python

import unittest

from etl.setup import ETLEnv
from etl.tests.test_tools import TestBase



# REVIEW Save sample test data for all institutions so that we can run tests even if API is down.


class TestPTH(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\ninfo:ark/67531/metapth304825 | local-cont-no: MAMU_2004-041 | ark: ark:/67531/metapth304825,[Speaker at Gallery Talk],"Mexic-Arte Museum (Austin, Tex.)","Photograph of a gallery talk at Mexican Report: Contemporary Art from Mexico, an exhibition hosted by the Mexic-Arte Museum in Austin, Texas. The speaker stands behind a podium. He holds a microphone. A painting hangs on the wall behind him. The painting features a pattern of gray squares.",2004-09-12,Photograph,Image,1 photograph : col. ; 4 x 6 in.,https://texashistory.unt.edu/ark:/67531/metapth304825/,"Mexican Report, Mexic-Arte Museum, Austin, Texas, 2004",No Language,"Into Modern Times, 1939-Present | 2004-09-12",Museum exhibits -- Texas -- Austin. | Arts and Crafts | Social Life and Customs - Fairs and Exhibitions | contemporary art,United States - Texas - Travis County - Austin,,,,,https://texashistory.unt.edu/ark:/67531/metapth304825/thumbnail\r\ninfo:ark/67531/metapth836721 | local-cont-no: Jaime_AC-2015-07-02 | ark: ark:/67531/metapth836721,"Oral History Interview with A.C. Jaime, July 2, 2015","Jaime, A. C. | Enriquez, Sandra | Robles, David","Interview with A.C. Jaime, an accountant from McAllen, Texas. Jaime was the first Mexican-American mayor of Pharr, Texas.  In his interview, Jaime discusses his early life, education,  political and professional career, the riots in Pharr, and race relations in the Rio Grande Valley",2015-07-02,Video,,"4 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836721/,https://crbb.tcu.edu/interviews/interview-with-a-c-jaime,English,,"People - Individuals | People - Ethnic Groups | oral histories | biographies | interviews | Jaime, A.C.",United States - Texas - Hidalgo County - Pharr | United States - Texas - Hidalgo County - McAllen,,,,,https://texashistory.unt.edu/ark:/67531/metapth836721/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    ETLEnv.instance().init_testing()

    unittest.main(TestPTH())
