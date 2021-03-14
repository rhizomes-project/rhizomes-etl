#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestPTH(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth836724 | info:ark/67531/metapth836724 | local-cont-no: Orendain_Antonio-2015-06-22,"Oral History Interview with Antonio Orendain, June 22, 2015","Enriquez, Sandra | Orendain, Antonio","Interview with Antonio Orendain, civil rights activist and founder of the Texas Farm Workers Union (TFWU), from McAllen, Texas.  In the interview, Orendain discusses his childhood and family background, working with Cesar Chavez and Dolores Huerta and the National Farm Workers Association in California, migrant farm workers, founding the TFWU, and his long career as a labor activist.",2015-06-22,Video,,"3 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836724/,https://crbb.tcu.edu/interviews/interview-with-antonio-orendain,Spanish,,"Orendain, Antonio | People - Ethnic Groups | People - Individuals | biographies | interviews | oral histories",United States - Texas - Hidalgo County - McAllen,,,,,https://texashistory.unt.edu/ark:/67531/metapth836724/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_tcu", format="csv", expected=expected)

    def test_partner(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth304825 | info:ark/67531/metapth304825 | local-cont-no: MAMU_2004-041,[Speaker at Gallery Talk],"Mexic-Arte Museum (Austin, Tex.)","Photograph of a gallery talk at Mexican Report: Contemporary Art from Mexico, an exhibition hosted by the Mexic-Arte Museum in Austin, Texas. The speaker stands behind a podium. He holds a microphone. A painting hangs on the wall behind him. The painting features a pattern of gray squares.",2004-09-12,Photograph,Image,1 photograph : col. ; 4 x 6 in.,https://texashistory.unt.edu/ark:/67531/metapth304825/,"Mexican Report, Mexic-Arte Museum, Austin, Texas, 2004",No Language,"2004-09-12 | Into Modern Times, 1939-Present",Arts and Crafts | Museum exhibits -- Texas -- Austin. | Social Life and Customs - Fairs and Exhibitions | contemporary art,United States - Texas - Travis County - Austin,,,,,https://texashistory.unt.edu/ark:/67531/metapth304825/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_mamu", format="csv", expected=expected)

    def test_collection(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth228033 | info:ark/67531/metapth228033 | issn: 1521-1606 | lccn: sn 98-2333 | oclc: 36932144,"ART LIES | Art Lies | Art Lies, A Contemporary Art Journal, No. 68 | Art Lies, Architecture Is Not Art | Art Lies, Volume 68, Spring/Summer 2011","Mueller, Kurt | Adams, Lauren | Beck, Logan | Blaylock, Cameron | Bonansinga, Kate | Bourbon, Matthew | Dearing, Harry | Evans, Ariel | Ewing, John | Judson, Ben | Masterpiece Litho, Inc. | Murray, Elizabeth | Puleo, Risa | Small Project Office | Tannahill, Kaylan","Journal containing essays, commentaries, and exhibition information regarding Texas artwork and other contemporary art issues.",2011,Journal/Magazine/Newsletter,Text,112 p. : col. ill. ; 28 cm.,https://texashistory.unt.edu/ark:/67531/metapth228033/,,English,,"Architecture | Art -- Texas -- Periodicals. | Art, Modern -- 20th century -- Periodicals. | Art, Modern -- 21st century -- Periodicals. | art analysis | art appreciation | journals",United States - Texas,,,,,https://texashistory.unt.edu/ark:/67531/metapth228033/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="collection_artl", format="csv", expected=expected)

    def test_partner_keywords(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth14379 | info:ark/67531/metapth14379 | oclc: 18095163,Brave Dick Dowling. | Robert Edward Lee. | Texas History Stories | Texas History Stories Number 6 | Texas History Stories: Brave Dick Dowling and Robert Edward Lee.,"Littlejohn, E. G., 1862-","Brief, subjective narratives of select events in the Mexican-American War, including biographical information about Dick Dowling and Robert E. Lee.",1901,Journal/Magazine/Newsletter,Text,"46, [2] p. : ill. ; 19 cm.",https://texashistory.unt.edu/ark:/67531/metapth14379/,,English,"1807/1870 | Civil War and Reconstruction, 1861-1876","Dowling, Dick, 1838-1867. | Dowling, Richard William (Dick) | Lee, Robert E. (Robert Edward), 1807-1870. | Lee, Robert Edward | Military and War | People - Individuals | Sabine Pass, Battle of, Tex., 1863. | Texas -- History -- Civil War, 1861-1865 -- Juvenile literature.",United States - Texas,,,,,https://texashistory.unt.edu/ark:/67531/metapth14379/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_unt", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestPTH())
