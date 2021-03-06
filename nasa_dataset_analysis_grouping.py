#!/usr/bin/env python3
__author__ = 'Felipe Cordeiro'
__title_ = 'nasa_dataset_analysis_grouping.py'
__usage__ = 'bin/spark-submit --master local <path>/nasa_dataset_analysis_grouping.py'

'''
This script puts the files into the folder to which belongs.
'''

from pyspark import SparkConf, SparkContext

logFile = "/Users/Felipe/PycharmProjects/spark_project/access_log_Jul95.log"
sc = SparkContext("local", "Nasa Dataset Analysis")
logData = sc.textFile(logFile)

def get_keys_values(x):
    key_value = []

    try:
        x = x.split("GET")[1].split("HTTP")[0].split("/")
    except IndexError:
        try:
            x = x.split("HEAD")[1].split("HTTP")[0].split("/")
        except IndexError:
            try:
                x = x.split("POST")[1].split("HTTP")[0].split("/")
            except IndexError:
                print("Without request or protocol: ", x)

    # There is no file in the path, it is a valid sequence of folders
    if x[0] == " " and x[-1] == " " and len(x) > 2:
        i = 0
        while i < len(x) - (len(x) - 1):
            try:
                item = str(x[i + 1]), str(x[i + 2])
                key_value.append(item)
            except UnicodeEncodeError:
                print ("Unicode Error: ", x)
            except IndexError:
                print("Seq.", x)
            i += 1

    # There is a file in the path
    if x[-1] != " ":
        try:
            item = str(x[len(x) - 2]), str(x[len(x) - 1])
            key_value.append(item)
        except UnicodeEncodeError:
            print ("Unicode Error: ", x)

    return key_value

keys_values_rdd = logData.flatMap(lambda line: get_keys_values(line))
result = keys_values_rdd.groupByKey()

for line in result.take(1):
    print ("- %s" % line[0])
    for line in line[1]:
        print ("-- %s" % line)

print result.toDebugString()