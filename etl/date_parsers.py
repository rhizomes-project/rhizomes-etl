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