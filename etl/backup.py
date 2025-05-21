#!/usr/bin/env python

import sys

from run import INST_ETL_MAP
from tools import get_current_metadata, MetadataWriter, RhizomeField


METADATA_MAP = {

    RhizomeField.TITLE: { "key": "dcterms:title", "selector": "@value" },
    RhizomeField.ALTERNATE_TITLES: { "key": "dcterms:alternative", "selector": "@value" },
    RhizomeField.AUTHOR_ARTIST: { "key": "dcterms:creator", "selector": "@value" },
    RhizomeField.IMAGES: { "key": "foaf:img", "selector": "@id" },
    RhizomeField.URL: { "key": "foaf:weblog", "selector": "@id" },
    RhizomeField.DESCRIPTION: { "key": "dcterms:description", "selector": "@value" },
    RhizomeField.SUBJECTS_TOPIC_KEYWORDS: { "key": "dcterms:subject", "selector": "@value" },
    RhizomeField.SEARCHABLE_DATE: { "key": "dcterms:date", "selector": "@value" },
    RhizomeField.RESOURCE_TYPE: { "key": "dcterms:type", "selector": "@value" },
    RhizomeField.DIGITAL_FORMAT: { "key": "dcterms:format", "selector": "@value" },
    RhizomeField.SOURCE: { "key": "dcterms:source", "selector": "@value" },
    RhizomeField.LANGUAGE: { "key": "dcterms:language", "selector": "@value" },
    RhizomeField.COLLECTION_NAME: { "key": "dcterms:contributor", "selector": "@value" },
    RhizomeField.ANNOTATES: { "key": "bibo:annotates", "selector": "@value" },
    RhizomeField.ACCESS_RIGHTS: { "key": "dcterms:accessRights", "selector": "@value" },

}


def get_key_values(obj, selector):

    if not obj:

        return None

    values = [ sub_obj[selector] for sub_obj in obj if sub_obj.get(selector) ]
    if not values:

      return None

    return "|".join(values)


# REVIEW: remove this:
sample_record = {
  "@context": "https://maas1848.umn.edu/api-context",
  "@id": "https://maas1848.umn.edu/api/items/99406",
  "@type": "o:Item",
  "o:id": 99406,
  "o:is_public": True,
  "o:owner": {
    "@id": "https://maas1848.umn.edu/api/users/2",
    "o:id": 2
  },
  "o:resource_class": None,
  "o:resource_template": {
    "@id": "https://maas1848.umn.edu/api/resource_templates/3",
    "o:id": 3
  },
  "o:thumbnail": None,
  "o:title": "036: Chicano Perspective",
  "thumbnail_display_urls": {
    "large": "https://maas1848.umn.edu/files/large/56f41f3b62c5f44b0e80285bc9820f7eb27b92b7.jpg",
    "medium": "https://maas1848.umn.edu/files/medium/56f41f3b62c5f44b0e80285bc9820f7eb27b92b7.jpg",
    "square": "https://maas1848.umn.edu/files/square/56f41f3b62c5f44b0e80285bc9820f7eb27b92b7.jpg"
  },
  "o:created": {
    "@value": "2021-10-01T11:33:10+00:00",
    "@type": "http://www.w3.org/2001/XMLSchema#dateTime"
  },
  "o:modified": {
    "@value": "2023-06-12T18:25:57+00:00",
    "@type": "http://www.w3.org/2001/XMLSchema#dateTime"
  },
  "o:media": [
    {
      "@id": "https://maas1848.umn.edu/api/media/99506",
      "o:id": 99506
    }
  ],
  "o:item_set": [
    
  ],
  "o:site": [
    {
      "@id": "https://maas1848.umn.edu/api/sites/1",
      "o:id": 1
    }
  ],
  "dcterms:title": [
    {
      "type": "literal",
      "property_id": 1,
      "property_label": "Title",
      "is_public": True,
      "@value": "036: Chicano Perspective"
    }
  ],
  "dcterms:creator": [
    {
      "type": "literal",
      "property_id": 2,
      "property_label": "Creator",
      "is_public": True,
      "@value": "El Teatro Campesino"
    }
  ],
  "foaf:weblog": [
    {
      "type": "uri",
      "property_id": 148,
      "property_label": "weblog",
      "is_public": True,
      "@id": "https://californiarevealed.org/islandora/object/cavpp%3A13985"
    }
  ],
  "dcterms:description": [
    {
      "type": "literal",
      "property_id": 4,
      "property_label": "Description",
      "is_public": True,
      "@value": "(1) Airdate May 22, 1983, recorded May 16, 1983. (2) Interview by Pilar Montoya with Luis Valdez; film footage from 'La Huelga' period -- Luis and Daniel Valdez perform, 'Zoot Suit' footage. (3) Audience reaction of 'Corridos' (San Francisco); cast of 'Corridos' talk. They are: Robert Beltran, Bel Hernandez, Richard Montoya and Irma La CuiCui Rangel. Sacramento: KXTV, Ch. 10"
    },
    {
      "type": "literal",
      "property_id": 4,
      "property_label": "Description",
      "is_public": True,
      "@value": "California Preservation Service (CAPS)"
    }
  ],
  "dcterms:subject": [
    {
      "type": "literal",
      "property_id": 3,
      "property_label": "Subject",
      "is_public": True,
      "@value": "Chicano Movement--California"
    }
  ],
  "dcterms:date": [
    {
      "type": "literal",
      "property_id": 7,
      "property_label": "Date",
      "is_public": True,
      "@value": "1983"
    }
  ],
  "dcterms:type": [
    {
      "type": "literal",
      "property_id": 8,
      "property_label": "Type",
      "is_public": True,
      "@value": "moving image"
    }
  ],
  "dcterms:format": [
    {
      "type": "literal",
      "property_id": 9,
      "property_label": "Format",
      "is_public": True,
      "@value": "Master"
    },
    {
      "type": "literal",
      "property_id": 9,
      "property_label": "Format",
      "is_public": True,
      "@value": "U-matic"
    }
  ],
  "dcterms:source": [
    {
      "type": "literal",
      "property_id": 11,
      "property_label": "Source",
      "is_public": True,
      "@value": "California Revealed from University of California, Santa Barbara, Department of Special Research Collections"
    }
  ],
  "dcterms:contributor": [
    {
      "type": "literal",
      "property_id": 6,
      "property_label": "Contributor",
      "is_public": True,
      "@value": "Digital Public Library of America (DPLA)"
    }
  ],
  "foaf:img": [
    {
      "type": "uri",
      "property_id": 154,
      "property_label": "image",
      "is_public": True,
      "@id": "https://calisphere.org/clip/500x500/76b9b0d1bdbff9bd877bafc5d8c22bd9"
    }
  ],
  "dcterms:accessRights": [
    {
      "type": "literal",
      "property_id": 47,
      "property_label": "Access Rights",
      "is_public": True,
      "@value": "Image is displayed for education and personal research only. For individual rights information about an item, please check the \\u201cDescription\\u201d field, or follow the link to the digital object on the content provider\\u2019s website for more information. Reuse of copyright protected images requires signed permission from the copyright holder. If you are the copyright holder of this item and its use online constitutes an infringement of your copyright, please contact us by email at rhizomes@umn.edu to discuss its removal from the portal."
    }
  ]
}



def do_backup(institution=None):

    # Filter by institution? If so, get institution name.
    if institution:

        process = INST_ETL_MAP[institution]
        institution = process(format="csv").get_collection_name()

    # Get all current metadata.
    metadata = get_current_metadata()

    # Output to csv here using MetadataWriter.
    #
    # Note:  Some records in the database seem to be corrupt (i.e., mostly or
    # completely empty - perhaps added by mistake?) Use data validation to
    # filter those out.
    #
    writer = MetadataWriter(format="csv", do_validate=True)

    writer.start_collection()
    record_count = 0

    # Parse each record and write it out.
    for record in metadata:

        writer.start_record()

        for rhizome_field, config in METADATA_MAP.items():

            key = config["key"]
            selector = config["selector"]

            obj = record.get(key)

            value = get_key_values(obj=obj, selector=selector)
            if value:

                writer.add_value(name=rhizome_field.value, value=value)

        # Filter by instition?
        #
        # Note: we *should* be able to search the API by institution name,
        # but I have not been able to get it to work.
        if institution and not writer.does_record_match(name=RhizomeField.COLLECTION_NAME.value, value=institution):

            pass

        else:

            writer.end_record()

            record_count += 1
            if record_count % 250 == 0:

                print(f"{record_count} written ...", file=sys.stderr)

    writer.end_collection()

    return 0


def do_usage():

    inst_list = list(INST_ETL_MAP.keys())
    inst_list = "|".join(inst_list)

    print("Usage: backup.py", file=sys.stderr)
    print(f"Usage: backup.py institution=[{inst_list}]", file=sys.stderr)


if __name__ == "__main__":    # pragma: no cover

    institution = sys.argv[ 1: ][0] if len(sys.argv) == 2 else None

    if institution and institution not in INST_ETL_MAP:

        do_usage()
        sys.exit(1)

    else:

        do_backup(institution=institution)
        sys.exit(0)
