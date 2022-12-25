import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
import string,re

wordbreak = r'[%s\s]+' % (re.escape(string.punctuation))

def get_data(in_directory):
    lines = spark.read.text(in_directory)
    
    return lines

def main(in_directory, out_directory):
    # Get data into a DataFrame 
    lines = get_data(in_directory)
    
    # Remove empty rows
    lines = lines.filter(lines.value != "")
    
    # Split lines into words using split and explode
    lines = lines.withColumn("word", functions.explode(functions.split(lines.value, wordbreak)))
    # Apply lowercase
    lines = lines.withColumn("word", functions.lower(lines.word))
    # Remove empty strings
    lines = lines.filter(lines.word != "")

    # Group, count, and sort
    words = lines.groupby("word").count().sort(functions.col("count").desc(), functions.col("word").asc())
    
    words.write.mode("overwrite").option("header", True).csv(out_directory)
    

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('wordcount').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)
