#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics
"""
This module is the spark streaming analysis process.


Usage:
    If used with dataproc:
        gcloud dataproc jobs submit pyspark --cluster <Cluster Name> twitterHTTPClient.py

    Create a dataset in BigQurey first using
        bq mk bigdata_sparkStreaming

    Remeber to replace the bucket with your own bucket name


Todo:
    1. hashtagCount: calculate accumulated hashtags count
    2. wordCount: calculate word count every 60 seconds
        the word you should track is listed below.
    3. save the result to google BigQuery

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import time
import subprocess
import re
from google.cloud import bigquery
from datetime import datetime

# global variables
bucket = "big_data_6893_project_bucket"    # TODO : replace with your own bucket name
output_directory_tweets = 'gs://{}/hadoop/tmp/bigquery/pyspark_output/tweets'.format(bucket)

# output table and columns name
output_dataset = 'twitter_stream'                     #the name of your dataset in BigQuery
output_table_tweets = 'tweets'
columns_name_tweets = ['tweet']

# parameter
IP = 'localhost'    # ip port
PORT = 9001       # port

STREAMTIME = 600          # time that the streaming process runs

# Helper functions
def saveToStorage(rdd, output_directory, columns_name, mode):
    """
    Save each RDD in this DStream to google storage
    Args:
        rdd: input rdd
        output_directory: output directory in google storage
        columns_name: columns name of dataframe
        mode: mode = "overwirte", overwirte the file
              mode = "append", append data to the end of file
    """
    if not rdd.isEmpty():
        (rdd.toDF( columns_name ) \
        .write.save(output_directory, format="json", mode=mode))


def saveToBigQuery(sc, output_dataset, output_table, directory):
    """
    Put temp streaming json files in google storage to google BigQuery
    and clean the output files in google storage
    """
    files = directory + '/part-*'
    subprocess.check_call(
        'bq load --source_format NEWLINE_DELIMITED_JSON '
        '--replace '
        '--autodetect '
        '{dataset}.{table} {files}'.format(
            dataset=output_dataset, table=output_table, files=files
        ).split())
    output_path = sc._jvm.org.apache.hadoop.fs.Path(directory)
    output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
        output_path, True)


def updateHelper(incomingNums, count):
    if count is None:
        count = 0
    return sum(incomingNums, count)


if __name__ == '__main__':
    # Spark settings
    conf = SparkConf()
    conf.setMaster('local[2]')
    conf.setAppName("TwitterStreamApp")

    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # create sql context, used for saving rdd
    sql_context = SQLContext(sc)

    # create the Streaming Context from the above spark context with batch interval size 5 seconds
    ssc = StreamingContext(sc, 5)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("~/checkpoint_TwitterApp")

    # read data from port 9001
    # we can use datastream as we're saving the entire tweet and not splitting into words so no need for flatMap as below
    dataStream = ssc.socketTextStream(IP, PORT)
    dataStream.pprint()

    # # save each line for datastream
    # tweets = dataStream.flatMap(lambda line: line)

    def saveTweets(rdd):
        saveToStorage(rdd, output_directory_tweets, columns_name_tweets, "overwrite")
    
    dataStream.foreachRDD(saveTweets)
    # tweets.foreachRDD(saveTweets)

    # start streaming process, wait for 600s and then stop.
    ssc.start()
    time.sleep(STREAMTIME)
    ssc.stop(stopSparkContext=False, stopGraceFully=True)

    # put the temp result in google storage to google BigQuery
    saveToBigQuery(sc, output_dataset, output_table_tweets, output_directory_tweets)
    # saveToBigQuery(sc, output_dataset, output_table_hashtags, output_directory_hashtags)
    # saveToBigQuery(sc, output_dataset, output_table_wordcount, output_directory_wordcount)


