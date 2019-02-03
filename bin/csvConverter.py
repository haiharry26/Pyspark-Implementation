# -*- coding: utf-8 -*-
"""

@author: Shreya
@date: Feb 3 2019

"""

import configparser

import logging
import logging.config
logging.config.fileConfig('../conf/logging.conf')
LOG = logging.getLogger('loggerAvro')

class converter():
    
    def __init__(self):
        self.property_file_name = "../config/config.ini"
        self.section_name = "DEFAULT"
        self.csv_file_name = None
        self.avro_location = None
        
        
        
    def read_properties(self):
        # config reader
        LOG.info('Initialising config reader for CSV to AVRO: ')
        
        LOG.info('Reading properties from config file: %s', self.property_file_name)
        config = configparser.RawConfigParser()
        config.read(self.property_file_name)
        LOG.info('Properties loaded in class variables.')

        self.csv_file_name = config.get(self.section_name, "FILE_NAME")
        self.avro_location = config.get(self.section_name, "AVRO_LOCATION")
        
        
    def csv_to_avro(self, spark):
        
        LOG.info('Reading csv file from: %s', self.csv_file_name)
        LOG.info('File format: com.databricks.spark.csv')
        df = spark.read.format("com.databricks.spark.csv").options(header='true', inferSchema='true').load(self.csv_file_name)
        LOG.info('Spark dataframe created.')
        
        LOG.info('Showing top 5 rows of dataframe: ')
        LOG.info('%s', df.show(5))
        
        LOG.info('Showing schema of the dataframe:')
        LOG.info('%s', df.printSchema())
        
        LOG.info('Creating avro from dataframe.')
        LOG.info('File format: com.databricks.spark.avro')
        df.write.format("com.databricks.spark.avro").mode("overwrite").save(self.avro_location)
        
        LOG.info('Avro created.')
        
        
        
    def runner(self, spark):
        LOG.info("Converting csv to avro:")
        
        LOG.info("Reading properties from config: ")
        self.read_properties()
        
        LOG.info("Converting csv to avro:")
        self.csv_to_avro(spark)
        
        LOG.info('Avro published. Returning from csv to avro.')