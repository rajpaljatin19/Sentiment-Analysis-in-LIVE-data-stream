# To run this on your local machine, you need to first run:
#    `$ nc -lk 9999`
# and then run the example
#    `$ bin/spark-submit
#    examples windowing.py localhost 9999 <window duration> [<slide duration>]`
#    spark-submit structured_network_wordcount_windowed.py localhost 9999 60


from __future__ import print_function

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

if __name__ == "__main__":
    if len(sys.argv) != 5 and len(sys.argv) != 4:
        msg = ("Usage: structured_network_wordcount_windowed.py <hostname> <port> "
               "<window duration in seconds> [<slide duration in seconds>]")
        print(msg, file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    windowSize = int(sys.argv[3])
    slideSize = int(sys.argv[4]) if (len(sys.argv) == 5) else windowSize
    if slideSize > windowSize:
        print("<slide duration> must be less than or equal to <window duration>", file=sys.stderr)
    windowDuration = '{} seconds'.format(windowSize)
    slideDuration = '{} seconds'.format(slideSize)

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCountWindowed")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .option('includeTimestamp', 'true')\
        .load()

    # Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows
    words = lines.select(
        explode(split(lines.value, ' ')).alias('word'),
        lines.timestamp
    )

    # Group the data by window and word and compute the count of each group
    windowedCounts = words.groupBy(
        window(words.timestamp, windowDuration, slideDuration),
        words.word
    ).count().orderBy('window')

    # Start running the query that prints the windowed word counts to the console
    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false')\
        .start()

    query.awaitTermination()
