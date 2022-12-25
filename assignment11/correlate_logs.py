import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
import re

line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred.
    Return None if regex doesn't match.
    """
    m = line_re.match(line)
    
    if m:
        # TODO
        return Row(hostname = m.group(1), bytes = m.group(2))
    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() objects
    log_lines = log_lines.map(line_to_row).filter(not_none)
    return log_lines

def main(in_directory):
    # Get data into a DataFrame using RDD
    logs = spark.createDataFrame(create_row_rdd(in_directory))

    #Group by hostname
    logs_g = logs.groupBy(logs.hostname).agg(functions.count(logs.hostname).alias('x'),
    					    functions.sum(logs.bytes).alias('y')).drop(logs.hostname)
    logs_g = logs_g.cache()
    			    
    n = logs_g.count()
    
    x = logs_g.agg(functions.sum(logs_g.x)).collect()[0][0]
    x2 = logs_g.agg(functions.sum(logs_g.x * logs_g.x )).collect()[0][0]    
    y = logs_g.agg(functions.sum(logs_g.y)).collect()[0][0]
    y2 = logs_g.agg(functions.sum(logs_g.y * logs_g.y )).collect()[0][0]
    xy = logs_g.agg(functions.sum(logs_g.x * logs_g.y )).collect()[0][0]
        
    
    r = (n*xy - x*y) / (((n*x2 - x**2)**0.5)*((n*y2-y**2)**0.5))

    print(f"r = {r:.6f}\nr^2 = {r*r:.6f}")
    # Built-in function should get the same results.
    #print(totals.corr('count', 'bytes'))


if __name__=='__main__':
    in_directory = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory)
