#!/usr/bin/env python

import requests
import sys
import time

from etl.etl_process import BaseETLProcess
from etl.setup import ETLEnv
from etl.tools import RhizomeField
from etl.date_parsers import get_date_first_four


# Start up secrets engine and get our api key.
etl_env = ETLEnv.instance()
etl_env.start()
api_key = etl_env.get_api_key(name="nhccnm")


native_field_map = {

    "sourceId":                             RhizomeField.ID,
    "title":                                RhizomeField.TITLE,
    "alt_title":                            RhizomeField.ALTERNATE_TITLES,
    "artist":                               RhizomeField.AUTHOR_ARTIST,
    "description":                          RhizomeField.DESCRIPTION,
    "subjects":                             RhizomeField.SUBJECTS_TOPIC_KEYWORDS,
    "dimensions":                           RhizomeField.DIMENSIONS,
    "displayDate":                          RhizomeField.DATE,
    "creditline":                           RhizomeField.SOURCE,
    "primaryMedia":                         RhizomeField.IMAGES,
    "access_rights":                        RhizomeField.ACCESS_RIGHTS

}

derived_field_map = {

    "web_url":                              RhizomeField.URL,
    "creation_year":                        RhizomeField.DATE,

}

field_map = derived_field_map | native_field_map

field_map_keys = list(field_map.keys())


ACCESS_RIGHTS = """Terms of Use for the National Hispanic Cultural Center, a division of the New Mexico Department of Cultural Affairs

COPYRIGHT

The contents of New Mexico Department of Cultural Affairs (DCA) Web sites, whether partial or otherwise, such as text, graphics, images, multimedia, software and other Web site content, are protected under both United States and foreign intellectual property laws. The compilation (meaning the collection, arrangement and assembly) of all content on this Web site is the exclusive property of the New Mexico Department of Cultural Affairs and is protected by U.S. and international copyright laws. Unauthorized use of the Web site content may violate copyright, trademark, and other laws.

DCA is committed to protecting the copyrights and other intellectual property rights of creative artists and others. DCA has taken all reasonable steps to ensure that reproductions of creative works on this Web site are produced according to applicable laws. Copyrights and other proprietary rights in the materials on this site may subsist in individuals and entities other than DCA. You hereby acknowledge and agree that, subject to valid third-party rights, DCA retains all intellectual property rights, including without limitation copyrights, trademarks, service marks, patents, and other proprietary rights, in and to the text, images, marks, data, and all other content contained in this Web site (collectively "Content"). DCA expressly prohibits the reproduction, distribution, transmission, sale, transfer, creation of derivative works, modification, public display, public performance, publication or any commercial exploitation of any Content on this Web site, except for the purposes of fair use as defined in the copyright laws and as described below.

GENERAL USE

With the exception of fair use, unauthorized reproduction, distribution, and exploitation of DCA Web site Content is not permitted. Anyone wishing to use any of the Content within this site for any purpose other than fair use as set forth below must request and receive prior written permission from DCA. DCA reviews all written requests and permission may be granted on a case-by-case basis at the sole discretion of DCA. A usage fee may be involved depending on the type and nature of the proposed use. Photographic and digital images of objects within the collections of DCA institutions may be available through the intellectual property manager for each institution; however, permissions must be secured from any third-party rightsholders.

FAIR USE

In accordance with the applicable copyright laws, any materials authored by DCA may be used for limited personal, educational, and non-commercial purposes without permission, provided that the source of the materials is cited. Unless otherwise noted, users who wish to download or print any materials authored by DCA from this or any Department Web site for such uses may do so without express permission. Users must cite the material as they would material from any printed work; the citation should include the URL from which the material is taken (such as "www.newmexicoculture.org") in addition to all copyright and other proprietary notices contained on the materials. Note, however, that certain works of art, as well as photographs of those works of art, may be protected by copyright, trademark, or related interests not owned by DCA. The responsibility for ascertaining whether any such rights exist and for obtaining all other necessary permissions remains solely with the user, and DCA will have no responsibility in connection with securing such necessary rights for any materials on the Web site. Any use of Content that does not qualify as fair use is subject to DCA prior written approval, and the user must request and receive such approval prior to any use.

NOTIFICATION OF CLAIMED INTELLECTUAL PROPERTY INFRINGEMENT

If you believe that your copyrighted work has been uploaded, posted or copied to any DCA Web site and is accessible on the Web site in a way that constitutes copyright infringement, please email: nhccvacollections@dca.nm.gov. Please provide DCA with a description of the copyrighted work you claim has been infringed, and a description of the activity that you claim to be infringing; identification of the URL or other specific location on this Web site where the material or activity you claim to be infringing is located or is occurring; you must include enough information to allow the department to locate the material or the activity; Your name, address, telephone number and, if you have one, your e-mail address; and an affidavit that the information you have provided in your notice is accurate and that you are either the copyright owner or are authorized to act on behalf of the copyright owner.

LINKS TO OTHER SITES

New Mexico DCA Web sites contain links to third party Web sites. These links are provided solely as a convenience to you and not as an endorsement by DCA. DCA is not responsible for the content of linked third-party sites and does not make any representations regarding the content or accuracy of materials on such third party Web sites. If you decide to access linked third party Web sites, you do so at your own risk."""


required_values = [
    # "ACCESSNO",
    # "TITLE",
    # "DESCRIP",
    # "OBJECT IMAGE"
]


# def is_record_valid(record):

#     for required_value in required_values:

#         if not record.get(required_value):

#             message = f"Missing {required_value}"

#             return False, message

#     return True, None

# def clean_value(value):

#     # Is this value on a blank line?
#     if not value:

#         return None

#     # This function can only clean strings.
#     if type(value) is not str:

#         return value

#     for bad_string in bad_strings:

#         value = value.replace(bad_string, " ")

#     return value.strip()

# def parse_date(value):

#     if not value:

#         return None

#     elif type(value) is not str:

#         return value

#     value = value.lower()

#     for invalid_prefix in invalid_prefixes:

#         if value.startswith(invalid_prefix):

#             value = value[ len(invalid_prefix) : ]
#             break

#     return clean_value(value=value)


def extract_value(object_, field):

    elt = object_.get(field)

    if elt:

        return elt["value"]

def extract_values(object_):

    record = {}

    for field in native_field_map:

        value = extract_value(object_=object_, field=field)

        if value:

            record[field] = value

    # Split title into title and alternate title
    title = record["title"]
    if title.startswith("Untitled ("):

        # Note: we are trimming parentheses from start & end of alt title.
        record["alt_title"] = title[ 10 : -1 ]
        record["title"] = title[ : 8 ]

    else:

        pos = title.find(" - ")
        if pos >= 0:

            record["alt_title"] = title[ pos + 3 : ]
            record["title"] = title[ : pos ]

    # Capitalize description.
    description = record["description"]
    record["description"] = description[0].upper() + description[ 1 : ]

    # Set the creditline.

    # Note: leaving this here in case NHCC changes their mind about the creditline.
    # source_ = record["creditline"]
    # pos = source_.find("CollectionNational")
    # if pos >= 0:

    #     pos += 10
    #     record["creditline"] = source_[ : pos ] + ", " + source_[ pos : ]

    record["creditline"] = "Rudy Padilla Paño Collection, Art Museum Permanent Collection at the National Hispanic Cultural Center"

    #
    # Fill in missing values..
    #

    source_id = record["sourceId"]

    # Make the unique ID be accession number (invno).
    record["sourceId"] = extract_value(object_=object_, field="invno")

    # Create the link to the landing page.
    record["web_url"] = f"https://collections.nhccnm.org/objects/{source_id}"

    # Create the image url.
    image_url = record["primaryMedia"]
    record["primaryMedia"] = image_url.replace("http://localhost/", "https://collections.nhccnm.org/")

    # Create subject as a list, including: medium, attributes, name
    medium = extract_value(object_=object_, field="medium")
    attributes = extract_value(object_=object_, field="attributes")
    name = extract_value(object_=object_, field="name")

    subjects = []
    if medium:

        subjects.append(medium)

    if attributes:

        subjects.append(attributes)

    if name:

        subjects.append(name)

    record["subjects"] = subjects

    # Handle date values
    if record.get("displayDate"):

        date_val = int(record["displayDate"])

        record["displayDate"] = date_val
        record["creation_year"] = date_val

    # Set the artist.
    # (Note: we are currently not receiving any artist value from NHCC's API.)
    record["artist"] = "Unidentified Artist"

    return record


class NHCCNMETLProcess(BaseETLProcess):

    def init_testing(self):

        pass

    def get_field_map(self):

        return field_map

    def get_collection_name(self):

        return "National Hispanic Cultural Center (NHCC)"

    def get_access_rights_stmt(self):

        return ACCESS_RIGHTS

    def get_date_parsers(self):

        return {
            r'^\d{4}':  get_date_first_four
        }

    def extract(self):

        page_num = 1
        records = []
        total_records = 1


        # REVIEW: remove this
        everything_buffer = ""


        while len(records) < total_records:

            url = f"https://collections.nhccnm.org/objects/json?key={api_key}&page={page_num}"

            response = requests.get(url=url, timeout=60)

            collection_json = response.json()
            total_records = collection_json["count"]

            # Parse all records.
            for object_ in collection_json["objects"]:

                source_id = object_["sourceId"]["value"]

                object_url = f"https://collections.nhccnm.org/objects/{source_id}/json?key={api_key}"
                response = requests.get(url=object_url, timeout=60)

                object_json = response.json()

                if len(object_json["object"]) > 1:

                    raise Exception("Found too many objects!")

                record = extract_values(object_=object_json["object"][0])
                records += [ record ]



                # REVIEW: remove this ...
                import json
                everything_buffer += json.dumps(object_json) + "\n"



            # Continue paging through records.
            page_num += 1



        # REVIEW: remove this ...
        print("\n\n\n" + everything_buffer)



        return records


if __name__ == "__main__":    # pragma: no cover

    from etl.run import run_cmd_line

    args_ = [ "nhccnm" ]

    if len(sys.argv) > 1:

        args_ += sys.argv[1:]

    run_cmd_line(args=args_)
