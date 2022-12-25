import sys
import re
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('page', types.StringType()),
    types.StructField('requests', types.LongType()),
    types.StructField('bytes', types.LongType())
])
""" Since removesuffix does no work on csil machines
def extractDate(filename):

    return filename.removesuffix(".gz")[-15:-4]
"""
def extractDate(filename):

    i = filename.rfind("-")
    date = filename[i-8:i-1]
    hour = filename[i:i+3]

    return date+hour

path_to_hour = functions.udf(extractDate, returnType=types.StringType())

def main(in_directory, out_directory):

    pagecounts = spark.read.csv(in_directory, schema=schema, sep=" ").withColumn("fileName", functions.input_file_name())

    pagecounts = pagecounts.filter(pagecounts.language.eqNullSafe("en"))
    #Link below is adapted for the negation in the filters
    #https://stackoverflow.com/questions/41775281/filtering-a-pyspark-dataframe-using-isin-by-exclusion
    pagecounts = pagecounts.filter(~pagecounts.page.startswith("Special:"))
    pagecounts = pagecounts.filter(~pagecounts.page.startswith("Main_Page"))
    pagecounts = pagecounts.withColumn("timestamp", path_to_hour(pagecounts.fileName))
    
    pagecounts = pagecounts.cache()
    
    maxrequests = pagecounts.groupBy(pagecounts.timestamp).agg(functions.max(pagecounts.requests))

    pagecounts = pagecounts.join(maxrequests, pagecounts.timestamp == maxrequests.timestamp, "inner").drop(maxrequests.timestamp)
    


    pagecounts = pagecounts.filter(pagecounts.requests == pagecounts["max(requests)"])

    pagecounts = pagecounts.select(pagecounts.timestamp ,pagecounts.page, pagecounts.requests)

    pagecounts = pagecounts.sort(pagecounts.timestamp ,pagecounts.page, pagecounts.requests)

    pagecounts.write.csv(out_directory + "-wikipedia", mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
