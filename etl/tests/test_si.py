#!/usr/bin/env python

import unittest

from etl.tests.test_tools import TestBase


class TestSI(TestBase):

    def test(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nedanmdm-siris_arc_216281,"Tomás Ybarra-Frausto research material on Chicano art, 1965-2004","Ybarra-Frausto, Tomás",,,Archival materials | Collection descriptions | Interviews | Video recordings,,,,,,1960s | 1970s | 1980s | 1990s | 2000s,Art | Artists | Household shrines | Religious life and customs | Santos (Art),Mexico | North America,"- Cite as: Tomás Ybarra-Frausto research material, 1965-2004. Archives of American Art, Smithsonian Institution | - Notes: Donated 1997, 2004 and 2009 by Tomás Ybarra-Frausto. | - Notes: Finding aid available. | - Notes: Tomás Ybarra-Frausto (1938-) is an art historian in New York, N.Y. Art historian Tomás Ybarra-Frausto is an authority on Chicano art and a bibliographer. In 1985, he co-authored with Shifra Goldman ""A Comprehensive Bibliography of Chicano Art, 1965-1981."" | - Repository Loc.: (1 hol) V31A3 | - Repository Loc.: (1 ov) dr068 | - Repository Loc.: (2 sols) V31A2 | - Repository Loc.: (28 bxs) VC5F3-VC6A3 | - Repository Loc.: (4 bxs, addition) VC11C3 | - Repository Loc.: (born digital files: network accessible) SAN | - Repository Loc.: Archives of American Art, Smithsonian Institution, Washington, D.C. 20560 | - Summary: Art historians have traditionally found the categorization of Chicano art a difficult task. Unsure whether to classify the work as ""American"" or ""Latin American,"" critics often ignored the work altogether. An outgrowth of this dilemma was the proliferation of artists, curators, and critics within the Chicano community, and the papers contain many original writings by Chicano artists about Chicano art, found in extensive files on artists that will be of particular significance to researchers. These often contain exhibition essays, dissertation proposals, and course outlines authored by the artists, along with the standard biographies, exhibition records, and reviews. Some of the files contain rare interviews conducted and transcribed by Ybarra-Frausto. Highlights include conversations with Carmen Lomas Garza, Amalia Mesa-Bains, and members of the Royal Chicano Air Force artist cooperative. | - Summary: As a member of several Chicano art organizations and institutions, Ybarra-Frausto kept active records of their operation. The extensive files on the Mexican Museum and Galerie de la Raza/Studio 24, both in San Francisco, not only chronicle the history of Chicano art through the records of exhibitions and programming, but also offer case studies on the development of non-profit art institutions. The files on artist cooperatives, organizations, and exhibition spaces cover several regions of the United States, but focus on California, Texas and New York. | - Summary: Deeply rooted in American history, ""El Movimiento,"" the Chicano movement, evolved from Mexican-Americans\' struggle for self-determination during the civil rights era of the 1960s. It began as a grassroots community effort that enlisted the arts in the creation of a united political and cultural constituency. Chicano artists, intellectuals, and political activists were instrumental in mobilizing the Mexican-American community for the cause of social justice, and the movement was shaped by the affirmation of a cultural identity that embraced a shared heritage with Mexico and the United States. | - Summary: His research material, dating from 1965 to 1996, are arranged in subject files containing original writings, notes, bibliographies compiled by Ybarra-Frausto and others, exhibition catalogues, announcements, newspaper clippings and other printed material, as well as slides and photographs. Many of these files also include interview transcripts and correspondence with prominent figures in the movement. While this research collection contextualizes Chicano art within the larger framework of Latino and Latin-American culture, the bulk of the files relates specifically to Chicano visual culture. The collection also contains pertinent documentation of the Chicano civil rights movement, material on Chicano poets and writers, and research files on the wider Hispanic community, but these also appear within the context of Chicano culture in general. | - Summary: Just as ""El Movimiento"" aimed to instruct and inspire through the recollection and conservation of culture, Ybarra-Frausto\'s own career as scholar and historian helped to shape the intellectual discourse of the Chicano art. As a leading historian and theoretician in the field of Chicano Studies, he has written extensively on the subject, and has been instrumental in defining the canons of Chicano art. His papers are accordingly rich and varied, and they will be of great use to future scholars. | - Summary: Prominent among the bibliographies are the many notes and drafts related to the publication of A Comprehensive Annotated Bibliography of Chicano Art, 1965-1981 (University of California, Berkeley, 1985), which Ybarra-Frausto co-authored with Shifra Goldman. Ybarra-Frausto\'s files on Goldman, like other files in the collection, document his close associations and collaborations with scholars. | - Summary: Spanning almost four decades of American culture from a Chicano perspective, these files have a unique historical value. The legacy of Chicano art and its contribution to the cultural landscape of this country, kept alive in Ybarra-Frausto\'s files, attests to the richness and diversity of American art. | - Summary: The research material of Tomás Ybarra-Frausto, amassed throughout his long and distinguished career as a scholar of the arts and humanities, documents the development of Chicano art in the United States. As community leader and scholar, Ybarra-Frausto played dual roles of active participant and historian in the Chicano movement, chronicling this unique political and artistic movement from its inception in the 1960s to the present day. | - Summary: Two notable events in the development of Chicano art were the 1982 Califas: Chicano Art and Culture in California seminar at the University of California at Santa Cruz, and the 1990 traveling exhibition Chicano Art: Resistance and Affirmation, 1965-1985 (CARA), of which Ybarra-Frausto served as organizer and catalogue essayist. His records document the planning and development of these seminal events. Ybarra-Frausto\'s files on folk art, altars, posters, murals, performance art, border art, Chicana feminist art, and Southwestern and Mexican imagery (both urban and rural expressions) mirror the diverse forms and subject matter of Chicano art.",,,,https://ids.si.edu/ids/download?id=NMAAHC-2012_36_4ab_001-000001.jpg | https://ids.si.edu/ids/download?id=NMAAHC-2012_36_4ab_002-000001.jpg\r\n\n'

        self.run_etl_test(institution="si", format="csv", expected=expected)

    def test_provider_aaa(self):

        expected = 'Resource Identifier,Title,Author/Artist,Description,Date,ResourceType,Digital Format,Dimensions,URL,Source,Language,Subjects (Historic Era),Subjects (Topic/Keywords),Subjects (Geographic),Notes,Copyright Status,Collection Information,Credit Line,Images\r\nedanmdm-siris_arc_211752,"Louis Pomerantz papers, 1937-1988, bulk 1950s-1988","Pomerantz, Louis",,,Archival materials | Collection descriptions | Interviews | Sound recordings,,,,,,1930s | 1940s | 1950s | 1960s | 1970s | 1980s,Art | Conservation and restoration | Flood damage | Museum conservation methods | Study and teaching | Technique,,"- Cite as: Louis Pomerantz papers, 1937-1988, bulk 1950s-1988. Archives of American Art, Smithsonian Institution | - Notes: Chicago art conservator Louis Pomerantz (1919-1988), established and operated the conservation lab at the Art Institute of Chicago and then maintained a private practice conducting conservation work for individual collectors and various museums and art institutions in the midwest. | - Notes: Funding for the processing of this collection is provided by the Terra Foundation for American Art, Smithsonian Institution. | - Notes: The Louis Pomerantz papers were donated to the Archives of American Art by Else Pomerantz. | - Occupation: Conservators Illinois Chicago | - Repository Loc.: (1 hol, #35) V3C8 | - Repository Loc.: (1 ov, #37) dr168 | - Repository Loc.: (2 sol, #33-34) V4D6 | - Repository Loc.: (33 bxs, #1-32, 36) VC1B3-VC1C3 | - Repository Loc.: (5 glass plate negs.) MGP1 | - Repository Loc.: Archives of American Art, Smithsonian Institution, Washington, D.C. 20560 | - Summary: Biographical material includes military and educational records, as well as resumés and references from various art institutions and individuals. | - Summary: Interviews include circa 9 radio station interviews on sound tape reels and sound cassettes of Pomerantz individually or with others, including a recording of a conversation regarding the Florence flood. | - Summary: Personal business records from the 1950s consist of receipts for conservation-related supplies and one folder of business tax records. | - Summary: Photographic material includes images demonstrating a wide variety of conservation techniques, including sets of slides used for lectures and presentations, and images of Pomerantz at work. Also found are photos of artists including Ulfert Wilke. Photographic media include black and white and color photos, slides, glass slides, X-rays and corresponding prints, negatives and 5 glass plate negatives. | - Summary: Pomerantz\'s professional correspondence is with other conservators including Anton J. Konrad, Nathan Stolow, and Jean Volkmer, conservation scientists such as Robert L. Feller, and people who assisted Pomerantz early in his career such as George Stout. Also documented is Pomerantz\'s relationship with the Smithsonian Institution Traveling Exhibition Service (SITES) which undertook his traveling Know What You See exhibition, his involvement with museums and other art institutions, and companies who developed and manufactured conservation equipment such as Eastman Kodak. | - Summary: Printed material primarily includes news clippings documenting Pomerantz\'s career up to and including the 1970s, clippings on conservation-related news, blank postcards of artwork, and two exhibition catalogs. | - Summary: Project/client files form the largest series and document Pomerantz\'s work, both in private practice and as conservator at the Art Institute of Chicago, through conditions reports, recommendations for and records of treatment, related correspondence, financial documentation, and photographic material. | - Summary: Teaching and reference files comprise material gathered by Pomerantz during participation in professional organizations and events, such as the American Institute for Conservation of Historic and Artistic Works, and the International Institute for Conservation of Historic and Artistic Works. Also found are subject files, consisting of reference material and correspondence, on a wide range of conservation-related subjects. | - Summary: The papers of Chicago art conservator, Louis Pomerantz, measure 34.2 linear feet and date from 1937 to 1988, with the bulk of the material dating from the 1950s-1980s. The papers document two principal aspects of Pomerantz\'s professional life: his conservation work for institutions and individuals, and the development of his professional expertise as documented through his writings and teachings, his continued conservation training, and his involvement in professional organizations. Files include scattered biographical material, professional correspondence, interviews, writings, project and client files, teaching and reference files, printed material, and photographic material primarily documenting conservation treatments and techniques. | - Summary: Writings and notes are by Pomerantz and include typescripts, notes and background material for lectures and papers delivered from the 1950s-1980s. Also found is a portfolio of his writings from 1962-1978, and a notebook Pomerantz compiled while working at the Rijksmuseum which includes notes, hand-drawn colored illustrations and photographs of conservation technique.",,,,\r\n\n'

        self.run_etl_test(institution="si", tag="provider_aaa", format="csv", expected=expected)


if __name__ == '__main__':    # pragma: no cover

    unittest.main(TestSI())
