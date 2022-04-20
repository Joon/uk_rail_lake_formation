#!/usr/bin/env python3

import logging
from time import sleep
import json
import boto3
from datetime import datetime

# dependencies are:
# stomp.py, termcolor

from termcolor import colored
import stomp

trains = {}
NETWORK_RAIL_AUTH = ('[Your Name]', '[Your password]')
RECONNECT_DELAY_SECS = 10

session = boto3.Session(
            aws_access_key_id='[Your AWS access key]',
            aws_secret_access_key='[Your AWS secret]'
        )

#Creating S3 Resource From the Session.
s3 = session.resource('s3')

class Listener(object):
    def __init__(self, mq):
        self._mq = mq
        self.colour = [lambda x: x, lambda x: colored(x, "white")]

    def on_heartbeat(self):
        print('Received a heartbeat')

    def on_heartbeat_timeout(self):
        print('ERROR: Heartbeat timeout')

    def on_error(self, frame):
        print('received an error "%s"' % frame.body)

    def on_disconnected(self):
        print('Disconnected, stopping')
        sleep(RECONNECT_DELAY_SECS)
        exit(-1)

    def on_connecting(self, host_and_port):
        print('Connecting to ' + host_and_port[0])

    def on_message(self, frame):
        headers, message = frame.headers, frame.body
        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])
        parsed = json.loads(message)
        now = datetime.now()
        stamp_str = now.strftime('%Y/%m/%d/%H/%M/events-%S') + ('-%02d' % (now.microsecond / 10000))
        print(f"{stamp_str} received {len(parsed)} messages, message size {len(message)}")

        object = s3.Object('train-data-landing', stamp_str)
        result = object.put(Body=message)

mq = stomp.Connection([('datafeeds.networkrail.co.uk', 61618)],
    keepalive=True, heartbeats=(15000, 15000))

mq.set_listener('', Listener(mq))

#mq.start()
print('connecting')
mq.connect(**{
    "username": NETWORK_RAIL_AUTH[0],
    "passcode": NETWORK_RAIL_AUTH[1],
    "wait": True,
    "client-id": NETWORK_RAIL_AUTH[0],
    })

print('subscribing')
mq.subscribe(**{
    "destination": "/topic/TRAIN_MVT_ALL_TOC",
    "id": 1,
    "ack": "client-individual",
    "activemq.subscriptionName": "joon_amzn",
    })

while mq.is_connected():
    sleep(1)