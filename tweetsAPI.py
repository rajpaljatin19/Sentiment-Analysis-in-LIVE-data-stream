#!/usr/bin/python

from __future__ import print_function
from lang_detect_exception import *
from confluent_kafka import Producer
import sys

import sys
import json
import time
from langdetect import detect

from satori.rtm.client import make_client, SubscriptionMode

endpoint = "wss://open-data.api.satori.com"
appkey = "A2cdb89fcc6cE8F7B6C3BB5A8de82bfF"
channel = "Twitter-statuses-sample"

###### Arguments
if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

broker = sys.argv[1]
topic = sys.argv[2]

conf = {'bootstrap.servers': broker}

# Create Producer instance
p = Producer(**conf)


####### Main Function
def main():
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))

    with make_client(endpoint=endpoint, appkey=appkey) as client:
        print('Connected to Satori RTM!')

        class SubscriptionObserver(object):
            def on_subscription_data(self, data):
                for message in data['messages']:
                   try:
                    if  'text' in list(message.keys()) and message["user"]["lang"] == "en" and detect(message["text"].encode('utf-8').strip()) == "en":
                         print(message ["text"])
                         p.produce(topic, value=message["text"].encode('utf-8').strip(),key=message['created_at'].encode('utf-8').strip() ,callback=delivery_callback)
                   except UnicodeDecodeError as err: 
                         sys.stderr.write('ignoring language error')
                   except Exception as err:
                         sys.stderr.write('ignoring language error')
                   except BufferError as err:
                         sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %len(p))
                         p.poll(0)
                p.flush()


        subscription_observer = SubscriptionObserver()
        client.subscribe(
            channel,
            SubscriptionMode.SIMPLE,
            subscription_observer)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
            sys.exit(0)



###### Main Function Call
if __name__ == '__main__':
    main()
