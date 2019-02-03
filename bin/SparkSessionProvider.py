# -*- coding: utf-8 -*-
"""

@author: Shreya
@date: Feb 3 2019

"""

import logging
import logging.config
logging.config.fileConfig('../conf/logging.conf')
LOG = logging.getLogger('loggerAvro')

from pyspark.sql import SparkSession

class spark_session_provider():
    
    def __init__(self):
        
        self.spark = None
        
    
    def spark_provider(self):
        
        self.spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("Avro_Reader").getOrCreate()
      
    
    def runner(self):
        
        LOG.info('Initiating Spark.')
        self.spark_provider()
        LOG.info('Spark Initialised. ')
        
        return self.spark