# -*- coding: utf-8 -*-
"""
Created on Fri Nov  1 16:43:41 2024

@author: cassi
"""

# import libraries
import time
import os

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub, SubscribeListener

# PubNub configuration
config = PNConfiguration()
config.subscribe_key = 'sub-c-c12423ae-e65d-429a-b550-4d92ed7c8026'
config.publish_key = 'pub-c-efb82edc-09a0-4fe0-bbf9-29c6b9cbed43'
config.uuid = 'example' 
pubnub = PubNub(config)

# subscribe to a channel
subscription = pubnub.channel('example').subscription()

# add a listener
subscription.on_message = lambda message: print(f'Message from {message.publisher}: {message.message}')
subscription.subscribe()

time.sleep(1)

# publish 
publish_result = pubnub.publish().channel("example").message("Hello from PubNub Python SDK").sync()

# this will replace default SubscribeListener with thing that will print out messages to console
class Listener(SubscribeListener):
    def status(self, pubnub, status):
        print(f'Status: \n{status.category.name}')

pubnub.add_listener(Listener())
# creates a subscription
subscription = pubnub.channel('example').subscription()
subscription.on_message = lambda message: print(f'Message from {message.publisher}: {message.message}')