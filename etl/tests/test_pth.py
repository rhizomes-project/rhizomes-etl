#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestPTH(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth836724 | info:ark/67531/metapth836724 | local-cont-no: Orendain_Antonio-2015-06-22,"Oral History Interview with Antonio Orendain, June 22, 2015","Enriquez, Sandra | Orendain, Antonio","Interview with Antonio Orendain, civil rights activist and founder of the Texas Farm Workers Union (TFWU), from McAllen, Texas.  In the interview, Orendain discusses his childhood and family background, working with Cesar Chavez and Dolores Huerta and the National Farm Workers Association in California, migrant farm workers, founding the TFWU, and his long career as a labor activist.",2015-06-22,Video,,"3 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836724/,https://crbb.tcu.edu/interviews/interview-with-antonio-orendain,Spanish,,"Orendain, Antonio | People - Ethnic Groups | People - Individuals | biographies | interviews | oral histories",United States - Texas - Hidalgo County - McAllen,,,,,https://texashistory.unt.edu/ark:/67531/metapth836724/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_tcu", format="csv", expected=expected)

    def test_collection(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth228033 | info:ark/67531/metapth228033 | issn: 1521-1606 | lccn: sn 98-2333 | oclc: 36932144,"ART LIES | Art Lies | Art Lies, A Contemporary Art Journal, No. 68 | Art Lies, Architecture Is Not Art | Art Lies, Volume 68, Spring/Summer 2011","Mueller, Kurt | Adams, Lauren | Beck, Logan | Blaylock, Cameron | Bonansinga, Kate | Bourbon, Matthew | Dearing, Harry | Evans, Ariel | Ewing, John | Judson, Ben | Masterpiece Litho, Inc. | Murray, Elizabeth | Puleo, Risa | Small Project Office | Tannahill, Kaylan","Journal containing essays, commentaries, and exhibition information regarding Texas artwork and other contemporary art issues.",2011,Journal/Magazine/Newsletter,Text,112 p. : col. ill. ; 28 cm.,https://texashistory.unt.edu/ark:/67531/metapth228033/,,English,,"Architecture | Art -- Texas -- Periodicals. | Art, Modern -- 20th century -- Periodicals. | Art, Modern -- 21st century -- Periodicals. | art analysis | art appreciation | journals",United States - Texas,,,,,https://texashistory.unt.edu/ark:/67531/metapth228033/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="collection_artl", format="csv", expected=expected)

if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestPTH())
