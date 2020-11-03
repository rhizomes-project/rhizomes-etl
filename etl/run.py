#!/usr/bin/env python


import setup
import pth_etl


class ETLProcess():

    # REVIEW TODO make this an ABC

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


def run_etl():

    etl_classes = [ PTHETLProcess ]
    for etl_class in etl_classes:

        etl_process = etl_class()

        etl_process.extract()
        etl_process.transform()
        etl_process.load()


if __name__ == "__main__":

    run_etl()