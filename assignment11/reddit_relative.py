import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    #types.StructField('year', types.IntegerType()),
    #types.StructField('month', types.IntegerType()),
])


def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=comments_schema)
    comments = comments.cache()
    comments = comments.select(comments.subreddit, comments.author, comments.score)

    # TODO
    
    # Calculate the average score for each subreddit, as before
    averages = comments.groupBy("subreddit").agg(functions.mean("score"))
    
    averages = averages.sort(averages.subreddit)
    
    # Exclude any subreddits with average score >= 0
    averages = averages.filter(averages['avg(score)']>= 0)
    
    # Create new column in comments df with average score of respective subreddit
    comments = comments.join(averages, comments.subreddit == averages.subreddit, how="inner").drop(averages.subreddit)
    #comments = comments.join(averages.hint("broadcast"), comments.subreddit == averages.subreddit, how="inner").drop(averages.subreddit)
    
    # Determining the relative score for each post
    comments = comments.withColumn('rel_score', comments.score / comments['avg(score)'])
    comments = comments.cache()
    # Grouping by subreddit and getting the best scores for each subreddit
    bestscores = comments.groupBy("subreddit").agg(functions.max("rel_score"))
    
   
    # Join based on subreddits and relative score
    comments = comments.join(bestscores, (bestscores["max(rel_score)"] == comments["rel_score"]) & (bestscores.subreddit == comments.subreddit), how='inner').drop(bestscores.subreddit)
    #comments = comments.join(bestscores.hint("broadcast"), (bestscores["max(rel_score)"] == comments["rel_score"]) & (bestscores.subreddit == comments.subreddit), how='inner').drop(bestscores.subreddit)
    
    best_author = comments.select(comments.subreddit, comments.author, comments.rel_score)
    
    best_author.write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('Reddit Relative Scores').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)
