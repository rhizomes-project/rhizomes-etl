#!/usr/bin/env python


import setup
import calisphere_etl
import dpla_etl
import pth_etl
import si_etl


# REVEW: TODO clean up "list" values so that they are easier to read (put each on its own line)
# REVIEW: TODO move each etl class (below) into the script it belongs to

# REVIEW: TODO add in URL mapping everywhere


class ETLProcess():

    # REVIEW TODO make this an ABC

    # def get_field_map(self):

    #     raise Exception("Base class get_field_map not defined.")

    def __init__(self):

        self.etl_env = setup.ETLEnv()
        self.etl_env.start()

        self.data = {}

    def extract(self):

        pass

    def transform(self):

        pass

    def load(self):

        pass


class PTHETLProcess(ETLProcess):

    def extract(self):

        self.data = pth_etl.extract()

    def transform(self):

        pth_etl.transform(data=self.data)

    def load(self):

        pth_etl.load(data=self.data)


class CalisphereETLProcess(ETLProcess):

    def extract(self):

        self.data = calisphere_etl.extract()

    def transform(self):

        calisphere_etl.transform(data=self.data)

    def load(self):

        calisphere_etl.load(data=self.data)


class DPLAETLProcess(ETLProcess):

    def extract(self):

        self.data = dpla_etl.extract()

    def transform(self):

        dpla_etl.transform(data=self.data)

    def load(self):

        dpla_etl.load(data=self.data)


class SIETLProcess(ETLProcess):

    def extract(self):

        self.data = si_etl.extract()

    def transform(self):

        si_etl.transform(data=self.data)

    def load(self):

        si_etl.load(data=self.data)


def run_etl():

    etl_classes = [ CalisphereETLProcess ]
    # etl_classes = [ DPLAETLProcess ]
    # etl_classes = [ PTHETLProcess ]
    # etl_classes = [ SIETLProcess ]

    for etl_class in etl_classes:

        etl_process = etl_class()

        etl_process.extract()
        etl_process.transform()
        etl_process.load()


if __name__ == "__main__":

    run_etl()
