from datetime import datetime
import re


def get_date_four_chars(date_val, offset=0):

    return date_val[offset : 4 + offset]

def get_date_first_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=0)

def get_date_second_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=1)

def get_date_third_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=2)

def get_date_fourth_four(date_val):

    return get_date_four_chars(date_val=date_val, offset=3)

def get_pth_year_range(date_val, offset=1):

    yr1 = int(date_val[1 : 5])
    yr2 = int(date_val[5 + offset : 9 + offset])

    return int(((yr2 + yr1) / 2) + .5)

def get_pth_year_range_skip2(date_val):

    return get_pth_year_range(date_val=date_val, offset=2)

def get_pth_year_range_offset6(date_val):

    return get_pth_year_range(date_val=date_val, offset=6)

def get_pth_date_range(date_val, offset=1):

    yr1 = int(date_val[1 : 5])
    yr2 = int(date_val[11 + offset : 15 + offset])

    return int(((yr2 + yr1) / 2) + .5)

def get_pth_date_range_2del(date_val):

    return get_pth_date_range(date_val=date_val, offset=2)

def get_pth_decade(date_val):

    yr = int(date_val.replace('X', '0'))
    return yr + 5

def get_pth_decade_estimate(date_val):

    return date_val[ : 3] + '5'


def get_date_via_regex(date_val, pattern, date_fmts):

    match = re.search(pattern, date_val)
    if not match:

        return None

    for date_fmt in date_fmts:

        date_val = date_val[match.start() : match.end()]

        try:

            dt = datetime.strptime(date_val, date_fmt)
            return dt.year

        except ValueError:

            pass


def get_date_mm_dd_yy(date_val):

    return get_date_via_regex(date_val=date_val, pattern=r'\d+[\-\/]\d+[\-\/]\d+', date_fmts=["%d/%m/%y", "%d-%m-%y"])

def get_date_mm_mon_yy(date_val):

    return get_date_via_regex(date_val=date_val, pattern=r'\d+\-[a-zA-Z]{3}\-\d+', date_fmts=["%d-%b-%y"])

def get_date_mon_yy(date_val):

    return get_date_via_regex(date_val=date_val, pattern=r'[a-zA-Z]{3}\-\d{2}', date_fmts=["%b-%y"])

def get_date_first_avail_4_digit_year(date_val):

    match = re.search(r'\d{4}', date_val)
    if not match:

        return None

    return date_val[match.start() : match.end()]
