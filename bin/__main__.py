# -*- coding: utf-8 -*-
"""

@author: Shreya
@date: Feb 3 2019

"""

import SparkSessionProvider
import csvConverter
import avroConverter

import logging
import logging.config

#initialising logger

logging.config.fileConfig('../conf/logging.conf')
LOG = logging.getLogger('loggerAvro')



if __name__ == '__main__':
    
    LOG.info("Code execution started. In main:")
    
    spark = SparkSessionProvider.spark_session_provider().runner()
    
    csvConverter.converter().runner(spark)       #Read and convert csv to avro
    
    avroConverter.converter().runner(spark)     #convert avro to pandas for processing
                                                # Create serialised file as well
