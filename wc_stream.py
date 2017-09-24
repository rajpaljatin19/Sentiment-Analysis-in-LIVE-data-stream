from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkConf, SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, Tokenizer,RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import concat, col, lit, monotonically_increasing_id
from pyspark.sql.types import *
import sys


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: ./wc_stream.py <bootstrap-servers> <source-topic> <staging-topic>
        """)
        sys.exit(0)

    bootstrapServers = sys.argv[1]
    src_topic = sys.argv[2]
    staging_topic = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("kafka_src_to_stage")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    # Reading positive & negetive words and labelling them
    pos = sc.textFile("/home/jatin/projects/app/ML/data/imdb/aclImdb/train/pos/*").repartition(10)
    neg = sc.textFile("/home/jatin/projects/app/ML/data/imdb/aclImdb/train/neg/*").repartition(10)

    pos_rdd = pos.map(lambda p: Row(text=p.encode('utf-8').strip(), label=float(1.0)))
    neg_rdd = neg.map(lambda n: Row(text=n.encode('utf-8').strip(), label=float(0.0)))

    pos_all = spark.createDataFrame(pos_rdd).withColumn("label", lit(1.0)).withColumn("id",monotonically_increasing_id())
    neg_all = spark.createDataFrame(neg_rdd).withColumn("label", lit(0.0)).withColumn("id",monotonically_increasing_id())

    training = pos_all.unionAll(neg_all)

    # Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="relevant_words")
    hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="rawFeatures")
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    #countVector = CountVectorizer(inputCol="relevant_words", outputCol="features", vocabSize=10000, minDF=2.0)
#    lr = LogisticRegression(maxIter=10, regParam=0.001)
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
#    pipeline_lr = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
    pipeline_nb = Pipeline(stages=[tokenizer, remover, hashingTF, idf, nb])

    # Fit the pipeline to training documents.
#    model_lr = pipeline_lr.fit(training)
    model_nb = pipeline_nb.fit(training)

    # Reading data from kafka-topic1
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", src_topic).load()
#    read_data=lines.selectExpr("CAST(value AS STRING) as text", "CAST(offset AS STRING) as offset_val")
    read_data=lines.selectExpr("CAST(value AS STRING) as text")
    print(read_data)

    # Make predictions on test documents and print columns of interest.
#    prediction_lr = model_lr.transform(test_ml)
    prediction_nb = model_nb.transform(read_data)
#    selected_lr = prediction_lr.select("id", "text", "probability" ,"prediction")
    selected_nb = prediction_nb.select("text", "probability", "prediction")
    print ("printing schema")
#    selected_lr.printSchema()
    selected_nb.printSchema()
#    read_data.printSchema()



    # Writing data to kafka-topic2
    write_stream = selected_nb.selectExpr("CONCAT (CAST(text AS STRING) , ':', CAST(prediction AS STRING) ) as value").writeStream.outputMode('append').format('kafka').option("kafka.bootstrap.servers", bootstrapServers).option("checkpointLocation", "/tmp/kafka_checkpoint").option("topic", staging_topic).start()
    write_stream.awaitTermination()
