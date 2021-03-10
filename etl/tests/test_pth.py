#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestPTH(TestBase):

    def test(self):

        # expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth304825 | info:ark/67531/metapth304825 | local-cont-no: MAMU_2004-041,[Speaker at Gallery Talk],"Mexic-Arte Museum (Austin, Tex.)","Photograph of a gallery talk at Mexican Report: Contemporary Art from Mexico, an exhibition hosted by the Mexic-Arte Museum in Austin, Texas. The speaker stands behind a podium. He holds a microphone. A painting hangs on the wall behind him. The painting features a pattern of gray squares.",2004-09-12,Photograph,Image,1 photograph : col. ; 4 x 6 in.,https://texashistory.unt.edu/ark:/67531/metapth304825/,"Mexican Report, Mexic-Arte Museum, Austin, Texas, 2004",No Language,"2004-09-12 | Into Modern Times, 1939-Present",Arts and Crafts | Museum exhibits -- Texas -- Austin. | Social Life and Customs - Fairs and Exhibitions | contemporary art,United States - Texas - Travis County - Austin,,,,,https://texashistory.unt.edu/ark:/67531/metapth304825/thumbnail\r\nark: ark:/67531/metapth836648 | info:ark/67531/metapth836648 | local-cont-no: Briones_Francisco-2015-06-25,"Oral History Interview with Francisco Briones, June 25, 2015","Briones, Francisco | Enriquez, Sandra | Robles, David","Interview with Francisco Briones, a civil rights activist from Alamo, Texas, near McAllen. In his interview, Briones discusses his early life, his education at Colegio Jacinto Treviño, and his activism in the Chicano movement in South Texas.",2015-06-25,Video,,"3 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836648/,https://crbb.tcu.edu/interviews/interview-with-francisco-briones,English,,"Briones, Francisco | People - Ethnic Groups | People - Individuals | biographies | interviews | oral histories",United States - Texas - Hidalgo County - Alamo,,,,,https://texashistory.unt.edu/ark:/67531/metapth836648/thumbnail\r\n\n'
        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth304825 | info:ark/67531/metapth304825 | local-cont-no: MAMU_2004-041,[Speaker at Gallery Talk],"Mexic-Arte Museum (Austin, Tex.)","Photograph of a gallery talk at Mexican Report: Contemporary Art from Mexico, an exhibition hosted by the Mexic-Arte Museum in Austin, Texas. The speaker stands behind a podium. He holds a microphone. A painting hangs on the wall behind him. The painting features a pattern of gray squares.",2004-09-12,Photograph,Image,1 photograph : col. ; 4 x 6 in.,https://texashistory.unt.edu/ark:/67531/metapth304825/,"Mexican Report, Mexic-Arte Museum, Austin, Texas, 2004",No Language,"2004-09-12 | Into Modern Times, 1939-Present",Arts and Crafts | Museum exhibits -- Texas -- Austin. | Social Life and Customs - Fairs and Exhibitions | contemporary art,United States - Texas - Travis County - Austin,,,,,https://texashistory.unt.edu/ark:/67531/metapth304825/thumbnail\r\nark: ark:/67531/metapth836648 | info:ark/67531/metapth836648 | local-cont-no: Briones_Francisco-2015-06-25,"Oral History Interview with Francisco Briones, June 25, 2015","Briones, Francisco | Enriquez, Sandra | Robles, David","Interview with Francisco Briones, a civil rights activist from Alamo, Texas, near McAllen. In his interview, Briones discusses his early life, his education at Colegio Jacinto Treviño, and his activism in the Chicano movement in South Texas.",2015-06-25,Video,,"3 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836648/,https://crbb.tcu.edu/interviews/interview-with-francisco-briones,English,,"Briones, Francisco | People - Ethnic Groups | People - Individuals | biographies | interviews | oral histories",United States - Texas - Hidalgo County - Alamo,,,,,https://texashistory.unt.edu/ark:/67531/metapth836648/thumbnail\r\nark: ark:/67531/metapth228033 | info:ark/67531/metapth228033 | issn: 1521-1606 | lccn: sn 98-2333 | oclc: 36932144,"ART LIES | Art Lies | Art Lies, A Contemporary Art Journal, No. 68 | Art Lies, Architecture Is Not Art | Art Lies, Volume 68, Spring/Summer 2011","Mueller, Kurt | Adams, Lauren | Beck, Logan | Blaylock, Cameron | Bonansinga, Kate | Bourbon, Matthew | Dearing, Harry | Evans, Ariel | Ewing, John | Judson, Ben | Masterpiece Litho, Inc. | Murray, Elizabeth | Puleo, Risa | Small Project Office | Tannahill, Kaylan","Journal containing essays, commentaries, and exhibition information regarding Texas artwork and other contemporary art issues.",2011,Journal/Magazine/Newsletter,Text,112 p. : col. ill. ; 28 cm.,https://texashistory.unt.edu/ark:/67531/metapth228033/,,English,,"Architecture | Art -- Texas -- Periodicals. | Art, Modern -- 20th century -- Periodicals. | Art, Modern -- 21st century -- Periodicals. | art analysis | art appreciation | journals",United States - Texas,,,,,https://texashistory.unt.edu/ark:/67531/metapth228033/thumbnail\r\nark: ark:/67531/metapth866432 | info:ark/67531/metapth866432 | lccn: sn88083592 | oclc: 17997374,"The Pharr Press | The Pharr Press (Pharr, Tex.), Vol. 65, No. 1, Ed. 1 Thursday, January 28, 1988","Glover, Lloyd H.","Quarterly newspaper from Pharr, Texas that includes local, state, and national news along with advertising.",1988-01-28,Newspaper,Text,four pages : ill. ; page 24 x 15 in.                                                                                                                                                                                                                                                                                     Scanned from physical pages.,https://texashistory.unt.edu/ark:/67531/metapth866432/,,English,"Into Modern Times, 1939-Present","Business, Economics and Finance - Advertising | Business, Economics and Finance - Communications - Newspapers | Business, Economics and Finance - Journalism | Hidalgo County (Tex.) -- Newspapers. | Pharr (Tex.) -- Newspapers. | Places - United States - Texas - Hidalgo County",United States - Texas - Hidalgo County - Pharr,,,,,https://texashistory.unt.edu/ark:/67531/metapth866432/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestPTH())
