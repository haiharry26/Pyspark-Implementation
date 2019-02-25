# -*- coding: utf-8 -*-
"""

@author: Shreya
@date: Feb 3 2019

"""


import pandas as pd
import numpy as np
import configparser

import logging
import logging.config
logging.config.fileConfig('../conf/logging.conf')
LOG = logging.getLogger('loggerAvro')


class converter():
    
    def __init__(self):
        self.property_file_name = "../conf/config.ini"
        self.section_name = "DEFAULT"
        self.avro_location = None
        self.spark_df = None
        
    def read_properties(self):
        # config reader
        LOG.info('Initialising config reader for AVRO to pandas dataframe: ')
        
        LOG.info('Reading properties from config file: %s', self.property_file_name)
        config = configparser.RawConfigParser()
        config.read(self.property_file_name)
        LOG.info('Properties loaded in class variables.')

        self.avro_location = config.get(self.section_name, "AVRO_LOCATION")
        self.serialised_pandas_df_location = config.get(self.section_name, "SERIALISED_PANDAS_DF_LOCATION")
    
    def avro_reader(self, spark):
        LOG.info('Reading avro file from: %s', self.avro_location)
        LOG.info('File format: com.databricks.spark.avro')
        self.spark_df = spark.read.format("com.databricks.spark.avro").load(self.avro_location)
        LOG.info('Spark dataframe created.')
        
        return self.spark_df
    
    
    def to_pandas_df(self, spark):
        
        LOG.info('Converting spark dataframe to panda dataframe:')
        self.df = self.spark_df.toPandas()  #Pandas 
        LOG.info('Pandas Dataframe created.')
        
        LOG.info('Serialising the pandas dataframe to use later.')
        self.df.to_pickle(self.serialised_pandas_df_location)
        LOG.info('Pndas Dataframe serialised successfully as ../data/data.pkl')
        
        
    def runner(self, spark):
        
        LOG.info('Reading properties:')
        self.read_properties()
        
        LOG.info('Read avro to a spark dataframe: ')
        self.avro_reader(spark)
        LOG.info('Read avro successfully.')
        
        LOG.info('Convert avro to Pandas dataframe: ')
        self.to_pandas_df(spark)
        LOG.info('Pandas Dataframe published.')
        
        LOG.info('Returning from avro converter.')
        
