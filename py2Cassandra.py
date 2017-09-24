#!/usr/bin/python
from cassandra.cluster import Cluster
from random import randint
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys, re
import getopt
import json
from pprint import pformat

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('demo')

##################
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


if __name__ == '__main__':
    optlist, argv = getopt.getopt(sys.argv[1:], 'T:')
    if len(argv) < 3:
        print_usage_and_exit(sys.argv[0])

    broker = argv[0]
    group = argv[1]
    topics = argv[2:]
    # Consumer configuration
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    # Check to see if -T option exists
    for opt in optlist:
        if opt[0] != '-T':
            continue
        try:
            intval = int(opt[1])
        except:
            sys.stderr.write("Invalid option value for -T: %s\n" % opt[1])
            sys.exit(1)

        if intval <= 0:
            sys.stderr.write("-T option value needs to be larger than zero: %s\n" % opt[1])
            sys.exit(1)

        conf['stats_cb'] = stats_cb
        conf['statistics.interval.ms'] = int(opt[1])

    # Create Consumer instance
    c = Consumer(**conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())
                message_txt=msg.value()
                replaced_mess=message_txt.replace('\n', ' ')
## From here
                ## Extracting sentiment
                word_list=replaced_mess.split()
                if re.findall(r'\d+', word_list[-1]) == ['1', '0']:
                   print "Positive"
                   sentiment="Positive"
                   ## Writing data
                   session.execute(
                   """
                   INSERT INTO tweets_sentiment (message, offset_value, topic, sentiment)
                   VALUES (%s, %s, %s, %s)
                   """,
                   (msg.value(), msg.offset(), msg.topic(), sentiment)
                   )
                elif re.findall(r'\d+', word_list[-1]) == ['0', '0']:
                   print "Negative"
                   sentiment="Negative"
                   ## Writing data
                   session.execute(
                   """
                   INSERT INTO tweets_sentiment (message, offset_value, topic, sentiment)
                   VALUES (%s, %s, %s, %s)
                   """,
                   (msg.value(), msg.offset(), msg.topic(), sentiment)
                   )
                else:
                   print "Invalid Sentiment"
## Till here

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    # Close down consumer to commit final offsets.
    c.close()
    sys.exit(0)
