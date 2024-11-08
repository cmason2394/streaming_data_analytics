# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 17:54:30 2024

@author: cassi
"""
# import libraries
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pubnub.callbacks import SubscribeCallback
from collections import deque
import pandas as pd

# Set the number of columns to display
pd.set_option('display.max_columns', None)

# PubNub configuration
pnconfig = PNConfiguration()
pnconfig.subscribe_key = 'sub-c-d00e0d32-66ac-4628-aa65-a42a1f0c493b'
pnconfig.uuid = 'camason' 
pubnub = PubNub(pnconfig)

# Define a listener for incoming tweets
class MySubscribeCallback(SubscribeCallback):
    # Initialize a deque with maximum length
    window_size = 100
    tweets_deque = deque(maxlen=window_size)
    
    def message(self, pubnub, message):
        # Print raw message to confirm data is received
        print("Received message:", message.message)

        # Extract and format data, checking each key to avoid KeyErrors
        tweet_data = {
            'Source': 'Twitter',
            'Text': message.message.get('text', ''),
            'Posted from': message.message.get('source', ''),
            'Tweeted from location': message.message.get('country', ''),
            'User name': message.message.get('user', {}).get('name', ''),
            'User profile location': message.message.get('user', {}).get('location', ''),
            'User follower count': message.message.get('user', {}).get('followers_count', 0),
            'Timestamp': message.message.get('created_at', '')
            }

        # Append tweet data to the class-level DataFrame and print to confirm
        MySubscribeCallback.tweets_deque.append(tweet_data)
      
        # Convert deque to dataframe for analysis
        tweets_df = pd.DataFrame(MySubscribeCallback.tweets_deque)
        print("current dataframe window: \n", tweets_df)

# Add listener and subscribe
pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels('pubnub-twitter').execute()

print(tweets_df.head())

# exploratory analysis

# Machine Learning Algorithm

# Test Machine Learning Algorithm

# visualize results