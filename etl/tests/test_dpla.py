from io import StringIO
import sys

import unittest

from etl.tests.test_tools import TestBase


class TestDPLA(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line\r\n3ef0ac8f4ac2890f6214f3e39e6d6d2c,Nuestra Señora de Guadalupe,"Márquez, Daniel, Artist | Márquez, Daniel, Artist","La Virgen de Guadalupe, seen from the hands up, in traditional attitude, wears a crown. The background is red. | The artist of any work retains all rights to that work. Copyright has not been assigned to the Regents of the University of California. The copyright law of the United States (Title 17, United States Code) governs the making of photocopies or other reproductions of copyrighted material. Under certain conditions specified in the law, libraries and archives are authorized to furnish a photocopy or other reproduction. One of these specified conditions is that the photocopy or reproduction is not to be \'used for any purpose other than private study, scholarship, or research.\'\' If a user makes a request for, or later uses, a photocopy or reproduction for purposes in excess of \'fair use,\' that user may be liable for copyright infringement. No further reproduction is permitted without prior written permission by the artist or copyright holder. Any requests for permission to reproduce this piece must be directed to: Self-Help Graphics & Art 3802 Cesar E. Chavez Avenue Los Angeles, CA 90063 For further information: (323) 881-6444 Fax: (323) 881-6447 info@selfhelpgraphics.com",1995,image,,,http://ark.cdlib.org/ark:/13030/hb6b69p4pk,Self-Help Graphics and Art archives,Spanish,,"Chicano art | Chicanos | Mexican American art | Mexican Americans | Prints | Guadalupe, Our Lady of",,,,"UC Santa Barbara, Library, Department of Special Research Collections",\r\n\n'

        self.run_etl_test(institution="dpla", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main()
