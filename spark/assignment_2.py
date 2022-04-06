import nltk
from nltk.corpus import stopwords
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import desc, lower, regexp_replace, explode
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from graphframes import *

spark = SparkSession.builder.appName('sg.edu.smu.is459.assignment2').getOrCreate()

# Load data
posts_df = spark.read.load('/quiz2/spark/hardwarezone.parquet')

# Clean the dataframe by removing rows with any null value
posts_df = posts_df.na.drop()

# Statistical information of the posts
author_count = posts_df.select('author','topic').distinct().groupBy('author').count()
author_count.sort(desc('count')).show()

print('# of topics: ' + str(posts_df.select('topic').distinct().count()))

# Find distinct users

author_df = posts_df.select('author').distinct()

print('Author number: ' + str(author_df.count()))

# Assign ID to the users
author_id = author_df.withColumn('id', monotonically_increasing_id())
#author_id.show()

# Construct connection between post and author
left_df = posts_df.select('topic', 'author') \
    .withColumnRenamed("topic","ltopic") \
    .withColumnRenamed("author","src_author")

right_df =  left_df.withColumnRenamed('ltopic', 'rtopic') \
    .withColumnRenamed('src_author', 'dst_author')

#  Self join on topic to build connection between authors
author_to_author = left_df. \
    join(right_df, left_df.ltopic == right_df.rtopic) \
    .select(left_df.src_author, right_df.dst_author)
edge_num = author_to_author.count()
print('Number of edges with duplicate: ' + str(edge_num))

# Convert it into ids
id_to_author = author_to_author \
    .join(author_id, author_to_author.src_author == author_id.author) \
    .select(author_to_author.dst_author, author_id.id) \
    .withColumnRenamed('id','src')

id_to_id = id_to_author \
    .join(author_id, id_to_author.dst_author == author_id.author) \
    .select(id_to_author.src, author_id.id) \
    .withColumnRenamed('id', 'dst')

id_to_id = id_to_id.filter(id_to_id.src > id_to_id.dst) \
    .groupBy('src','dst') \
    .count() \
    .withColumnRenamed('count', 'n')


id_to_id = id_to_id.filter(id_to_id.n >= 5)

#id_to_id.cache()

print("Number of edges without duplciate: " + str(id_to_id.count()))

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkopoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir('/quiz2/spark/spark-checkpoint')

# The rest is your work, guys
# ......
# How large are the communities?
result = graph.connectedComponents()
#result.show()

print("Number of connected components: " + str(result.select("component").distinct().count()))

result.write.csv('/quiz2/spark/cc.csv', mode='overwrite')

# What are the key words of the community?
content = posts_df.select(lower(regexp_replace('content', "[^a-zA-Z\\s]", "")).alias('content'))
tokenizer = Tokenizer(outputCol="words")
tokenizer.setInputCol("content")
df = tokenizer.transform(content)
list_of_stopwords = list(stopwords.words('english')) + ['', 'u']
remover = StopWordsRemover(stopWords=list_of_stopwords)
remover.setInputCols(["words"]).setOutputCols(["cleaned_words"])
df2 = remover.transform(df)
df2.withColumn('word', explode('cleaned_words')).groupBy('word').count().sort('count', ascending=False).show()

# How cohesive are the communities?
result = graph.triangleCount()
#result.show()
result_rdd = result.rdd
map_count = result_rdd.map(lambda x: (x[0], 1))
(sum, count) = map_count.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print(sum, count)
print("Number of triangle counts: " + str(sum/count))