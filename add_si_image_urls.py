#!/usr/bin/env python

import csv
import sys


from etl.etl_si import get_image_urls


fieldnames = [ 'Resource Identifier', 'Title', 'Author/Artist', 'Description', 'Date', 'ResourceType', 'Digital Format', 'Dimensions', 'URL', 'Source', 'Language', 'Subjects (Historic Era)', 'Subjects (Topic/Keywords)', 'Subjects (Geographic)', 'Notes', 'Copyright Status', 'Collection Information', 'Credit Line', 'Images' ]


if __name__ == "__main__":

    input_file = sys.argv[1]
    first_row = int(sys.argv[2])

    row_len = len(fieldnames)

    row_num = 0
    batch_size = 250

    with open(input_file, 'r') as csvfile:

        datareader = csv.reader(csvfile)
        datawriter = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        datawriter.writeheader()

        first_row = True

        for row in datareader:

            if first_row:

                first_row = False
                continue

            id_ = row[0]

            if not id_:

                pass

            if row_num >= first_row and row_num < (first_row + batch_size):

                urls = row[row_len - 1]
                if not urls:

                    urls = get_image_urls(id_=id_)
                    if urls:

                        urls = '|'.join(urls)
                        row.append(urls)

            row_data = { name : row[idx] for idx, name in enumerate(fieldnames) }

            datawriter.writerow(row_data)
            row_num += 1
