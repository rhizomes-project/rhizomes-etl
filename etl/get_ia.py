#!/usr/bin/env python


from internetarchive import search_items

# from setup import ETLEnv


# etl_env = ETLEnv()
# etl_env.start()
# api_key = etl_env.ia_api_key

METADATA_KEY = {
    # 'title': 26,
    'identifier': 26,
    'mediatype': 26,
    'collection': 26,
    'creator': 21,
    'date': 14,
    'description': 26,
    'language': 13,
    'scanner': 23,
    'subject': 25,
    'publicdate': 26,
    'uploader': 26,
    'addeddate': 26,
    'curation': 23,
    # 'backup_location': 20,
    'sound': 1,
    'color': 1,
    'sponsor': 2,
    'credits': 1,
    'year': 3,
    'publisher': 2,
    'file': 1,
    'rssfeed': 1,
    'source': 1,
    'external_metadata_update': 7,
    'identifier-access': 3,
    'identifier-ark': 3,
    'ppi': 3,
    'ocr': 3,
    'repub_state': 2,
    'licenseurl': 6,
    # 'page_number_confidence': 1,
    'duration': 1,
    'runtime': 1,
    'youtube-height': 1,
    'youtube-id': 1,
    'youtube-n-entries': 1,
    'youtube-playlist': 1,
    'youtube-playlist-index': 1,
    'youtube-uploader': 1,
    'youtube-uploader-id': 1,
    'youtube-view-count': 1,
    'youtube-webpage-url': 1,
    'youtube-width': 1,
    'associated-names': 1,
    'boxid': 1,
    'camera': 1,
    'collection_set': 1,
    'contributor': 1,
    'isbn': 1,
    'lccn': 1,
    'old_pallet': 1,
    'openlibrary_edition': 1,
    'openlibrary_work': 1,
    'operator': 1,
    'partner': 1,
    'rcs_key': 1,
    'scanningcenter': 1,
    'scribe3_search_catalog': 1,
    'scribe3_search_id': 1,
    'tts_version': 1,
    'access-restricted-item': 1,
    'imagecount': 1,
    'scandate': 1,
    'notes': 1,
    'republisher_operator': 1,
    'republisher_date': 1,
    'republisher_time': 1,
    'foldoutcount': 1,
    'external-identifier': 1,
    'loans__status__status': 1,
    'loans__status__num_loans': 1,
    'loans__status__num_waitlist': 1,
    'loans__status__num_history': 1,
    'scanfee': 1,
    'invoice': 1,
    'loans__status__last_loan_date': 1,
    'sponsordate': 1
}


def print_val(key, value):

    print(f"{key}: {value}")


def print_metadata(item):

    print("")
    # print(item.metadata)

    metadata = item.metadata

    print_val(key="identifier", value=metadata['identifier'])
    print_val(key="details_url", value=item.urls.details)
    # print_val(key="download_url", value=item.urls.download)

    for key in METADATA_KEY.keys():

        value = metadata.get(key)
        if value:

            print_val(key=key, value=value)


if __name__ == "__main__":

    # items = search_items('identifier:chicano').iter_as_items()
    # for item in items:
    #     print(si['identifier'])


    # metadata = {}

    items = search_items(query="chicano").iter_as_items()
    for idx, item in enumerate(items):

        if idx > 25:

            break

        print_metadata(item=item)


    #     for key in item.metadata.keys():

    #         cnt = metadata.get(key, 0)
    #         metadata[key] = cnt + 1

    # print(metadata)
