#!/usr/bin/env python


import setup
from calisphere_etl import CalisphereETLProcess
from dpla_etl import DPLAETLProcess
from pth_etl import PTHETLProcess
# from si_etl import SIETLProcess


# REVEW: TODO clean up "list" values so that they are easier to read (put each on its own line)
# REVIEW: TODO add in URL mapping everywhere


# class SIETLProcess(ETLProcess):

#     def extract(self):

#         self.data = si_etl.extract()

#     def transform(self):

#         si_etl.transform(data=self.data)

#     def load(self):

#         si_etl.load(data=self.data)


def run_etl():

    format = "csv"

    # etl_classes = [ CalisphereETLProcess ]
    # etl_classes = [ DPLAETLProcess ]
    etl_classes = [ PTHETLProcess ]
    # etl_classes = [ SIETLProcess ]

    for etl_class in etl_classes:

        etl_process = etl_class(format=format)

        data = etl_process.extract()
        etl_process.transform(data=data)
        etl_process.load(data=data)


if __name__ == "__main__":

    run_etl()
