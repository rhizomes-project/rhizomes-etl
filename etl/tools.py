#!/usr/bin/env python


import pdb


def get_pretty_value(value):

    if type(value) is list:

        if len(value) == 1:

            return value[0]

        else:

            buf = ""
            for tmp in value:

                if type(tmp) is not str:


                    pdb.set_trace()


                else:

                    buf += "\n\t" + tmp

            return buf

    elif type(value) is dict:


        pdb.set_trace()

        return ""


    else:

        return value


def pretty_print(name, value):

    print(f"{name}: {get_pretty_value(value=value)}")