# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 16:02:27 2024

@author: cassi
"""
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pubnub.callbacks import SubscribeCallback

from datetime import datetime, timedelta
import threading

import streamz
import pandas as pd
import matplotlib.pyplot as plt
   

# PubNub configuration
config = PNConfiguration()
config.subscribe_key = 'sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8'
config.publish_key = 'pub-c-efb82edc-09a0-4fe0-bbf9-29c6b9cbed43'
config.uuid = 'camason' 
pubnub = PubNub(config)

# create a stream
source = streamz.Stream()

# Global DataFrame to store recent data (sliding window)
recent_data = pd.DataFrame()

# Global dataframe to store aggregates from the past hour
agg_df = pd.DataFrame()

def process_data(data):
    global recent_data
    new_row = pd.DataFrame([data])
    
    # Append new row
    recent_data = pd.concat([recent_data, new_row], ignore_index=True)
    
    return recent_data
    
# Function to perform aggregation and clearing of old data, triggered every hour
def aggregate_and_save(data):
    # Aggregate statistics for each stock symbol
    agg_data = data.groupby('symbol').agg(
        total_orders=('order_quantity', 'sum'),
        max_price=('bid_price', 'max'),
        min_price=('bid_price', 'min'),
        mean_price=('bid_price', 'mean')
    ).reset_index()
    
    # Add a timestamp for when this aggregation was calculated
    agg_data['timestamp'] = datetime.now()
    
    # Save the aggregated data to the agg_df dataframe
    global agg_df
    agg_df = pd.concat([agg_df, agg_data], ignore_index=True)
    
    # Clear out data in `recent_data` older than one hour
    global recent_data
    one_hour_ago = datetime.now() - timedelta(hours=1)
    recent_data = recent_data[recent_data['timestamp'] >= one_hour_ago]
    
    # Print the updated aggregated dataframe for verification
    print("Aggregated Data:", agg_df)

# create a PubNub callback class to push data into the streamz stream
class MySubscribeCallback(SubscribeCallback):
    def message(self, pubnub, message):
        # convert timetoken from 100-nanosecond intervals since 01/01/1970 to a date time
        epoch_time = int(message.timetoken)/1e7
        readable_timestamp = datetime.fromtimestamp(epoch_time)
        
        # Add converted timestamp directly to the message dictionary
        message.message['timestamp'] = readable_timestamp
        
        # Send the message content to the streamz stream
        source.emit(message.message)

# verify data is coming into stream, uncomment to verify
#source.sink(print)
#source.map(lambda x: f"Message: {x['message']}, Timestamp: {x['timestamp']}").sink(print)

'''
# function to remove the 'timestamp' key from the message content
def remove_original_timestamp(data):
    # Remove the 'timestamp' attribute within the message if it exists
    data['message'].pop('timestamp', None)
    return data
'''

# Use Streamz to apply the removal function to each incoming data point
source.map(process_data).sink(print)

# Schedule the aggregation function to run every hour
source.timed_window(3600).map(lambda x: aggregate_and_save(recent_data))

# create a thread for the data stream
def data_stream_thread():
    # Instantiate the SubscribeCallback to start listening and pushing data into the streamz stream
    pubnub.add_listener(MySubscribeCallback()) 
    pubnub.subscribe().channels('pubnub-market-orders').execute()
    
# Start thread, data stream running
stream_thread = threading.Thread(target=data_stream_thread)
stream_thread.start()

# add a column that calculated price per stock by dividing bid_price by order_quantity
