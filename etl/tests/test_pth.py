#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestPTH(TestBase):

    def test_partner(self):

        # REVIEW: Not sure why some content is getting case changed to lower.

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth304825 | info:ark/67531/metapth304825 | local-cont-no: MAMU_2004-041,[Speaker at Gallery Talk],"Mexic-Arte Museum (Austin, Tex.)","Photograph of a gallery talk at Mexican Report: Contemporary Art from Mexico, an exhibition hosted by the Mexic-Arte Museum in Austin, Texas. The speaker stands behind a podium. He holds a microphone. A painting hangs on the wall behind him. The painting features a pattern of gray squares.",2004-09-12,Photograph,Image,1 photograph : col. ; 4 x 6 in.,https://texashistory.unt.edu/ark:/67531/metapth304825/,"Mexican Report, Mexic-Arte Museum, Austin, Texas, 2004",No Language,"2004-09-12 | Into Modern Times, 1939-Present",Arts and Crafts | Museum exhibits -- Texas -- Austin. | Social Life and Customs - Fairs and Exhibitions | contemporary art,United States - Texas - Travis County - Austin,,,,,https://texashistory.unt.edu/ark:/67531/metapth304825/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_mamu", format="csv", expected=expected)

    def test_collection(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth228033 | info:ark/67531/metapth228033 | issn: 1521-1606 | lccn: sn 98-2333 | oclc: 36932144,"ART LIES | Art Lies | Art Lies, A Contemporary Art Journal, No. 68 | Art Lies, Architecture Is Not Art | Art Lies, Volume 68, Spring/Summer 2011","Mueller, Kurt | Adams, Lauren | Beck, Logan | Blaylock, Cameron | Bonansinga, Kate | Bourbon, Matthew | Dearing, Harry | Evans, Ariel | Ewing, John | Judson, Ben | Masterpiece Litho, Inc. | Murray, Elizabeth | Puleo, Risa | Small Project Office | Tannahill, Kaylan","Journal containing essays, commentaries, and exhibition information regarding Texas artwork and other contemporary art issues.",2011,Journal/Magazine/Newsletter,Text,112 p. : col. ill. ; 28 cm.,https://texashistory.unt.edu/ark:/67531/metapth228033/,,English,,"Architecture | Art -- Texas -- Periodicals. | Art, Modern -- 20th century -- Periodicals. | Art, Modern -- 21st century -- Periodicals. | art analysis | art appreciation | journals",United States - Texas,,,,,https://texashistory.unt.edu/ark:/67531/metapth228033/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="collection_artl", format="csv", expected=expected)

    def test_partner_keywords(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth3405 | info:ark/67531/metapth3405 | issn: 0362-4781 | oclc: 2309724,Made up record about Chicano Art | Texas Register,Texas. Secretary of State.,"A weekly publication, the Texas Register serves as the journal of state agency rulemaking for Texas. Information published in the Texas Register includes proposed, adopted, withdrawn and emergency rule actions, notices of state agency review of agency rules, governor\'s appointments, attorney general opinions, and miscellaneous documents such as requests for proposals. After adoption, these rulemaking actions are codified into the Texas Administrative Code.",1991-01-01,Journal/Magazine/Newsletter,Text,62 p. ; 28 cm.,https://texashistory.unt.edu/ark:/67531/metapth3405/,,English,1991-01,"Delegated legislation -- Texas -- Periodicals. | Government and Law - Texas Laws and Regulations | Texas -- Politics and government -- Handbooks, manuals, etc. | Texas -- Politics and government -- Periodicals. | laws | regulations",United States - Texas,,,,,https://texashistory.unt.edu/ark:/67531/metapth3405/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_untgd", format="csv", expected=expected)

    def test_collection_resource_type(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth222215 | info:ark/67531/metapth222215 | local-cont-no: HPLM_MSS282-b2-f08-06,[Band of children on Fiestas Patrias float],,"Photograph of a band of children on a Fiestas Patrias float, 1982. A sign on the float reads, ""Ripley.""",1982,Photograph,Image,1 photograph : b&amp;w ; 5 x 7 in.,https://texashistory.unt.edu/ark:/67531/metapth222215/,,No Language,"Into Modern Times, 1939-Present",Fiestas Patrias International Parade | Hispanic Americans. | Parades. | People - Ethnic Groups - Hispanics | downtown | people,United States - Texas - Harris County - Houston,,,,,https://texashistory.unt.edu/ark:/67531/metapth222215/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="collection_mafp", format="csv", expected=expected)

    def test_partner_unta(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth24471 | info:ark/67531/metapth24471 | local-cont-no: PICT3411,Colima Fest | [Side view of man speaking into microphone],"Castillo, JosÃ© L.","More than 100 Chicanos from Dallas contributed to the Mexican state of Colima to be entered in the next Guinness book of world records for having created the world\'s largest lemonade at 3500 liters.\r\n\r\n20,000 lemons, or one ton, from the valleys of Colima along with 3750 liters of water and 56 liters of syrup beat out the previous record holder of 2500 liters that was created in Victoria, Australia in 1996.",2006-08-06,Photograph,Image,"1 photograph : digital, col.",https://texashistory.unt.edu/ark:/67531/metapth24471/,,Spanish,"2006-08-06 | Into Modern Times, 1939-Present",Arts and Crafts,United States - Texas - Dallas County - Dallas,,,,,https://texashistory.unt.edu/ark:/67531/metapth24471/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_unta", format="csv", expected=expected)

    def test_partner_title_search(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth224692 | info:ark/67531/metapth224692 | local-cont-no: 1227_ReligiousArt_1973_PR,Christmas at the Museum [Press Release],Dallas Museum of Fine Arts,"Press release related to the exhibition The Hand and the Spirit: Religious Art in America, 1700–1900, December 10, 1972–January 14, 1973, at the Dallas Museum of Fine Arts.",1972,Text,Text,2 p. ; 8 x 11 in.,https://texashistory.unt.edu/ark:/67531/metapth224692/,"The Hand and the Spirit: Religious Art in America, 1700–1900, December 10, 1972–January 14, 1973",English,"Into Modern Times, 1939-Present",Museum exhibits -- Texas -- Dallas. | Press releases | artworks | exhibitions,United States - Texas - Dallas County - Dallas,,,,,https://texashistory.unt.edu/ark:/67531/metapth224692/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="partner_dma", format="csv", expected=expected)

    def test_creator_search(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nark: ark:/67531/metapth836724 | info:ark/67531/metapth836724 | local-cont-no: Orendain_Antonio-2015-06-22,"Oral History Interview with Antonio Orendain, June 22, 2015","Jimenez, Luis | Orendain, Antonio","Interview with Antonio Orendain, civil rights activist and founder of the Texas Farm Workers Union (TFWU), from McAllen, Texas.  In the interview, Orendain discusses his childhood and family background, working with Cesar Chavez and Dolores Huerta and the National Farm Workers Association in California, migrant farm workers, founding the TFWU, and his long career as a labor activist.",2015-06-22,Video,,"3 videos : sd., col. ; digital | Video",https://texashistory.unt.edu/ark:/67531/metapth836724/,https://crbb.tcu.edu/interviews/interview-with-antonio-orendain,Spanish,,"Orendain, Antonio | People - Ethnic Groups | People - Individuals | biographies | interviews | oral histories",United States - Texas - Hidalgo County - McAllen,,,,,https://texashistory.unt.edu/ark:/67531/metapth836724/thumbnail\r\n\n'

        self.run_etl_test(institution="pth", tag="creator_search", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestPTH())
