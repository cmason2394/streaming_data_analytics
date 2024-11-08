# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 16:58:31 2024

@author: cassi
"""
# import libraries
import threading
from datetime import timedelta, datetime

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pubnub.callbacks import SubscribeCallback

import streamz
import pandas as pd
import matplotlib.pyplot as plt
   

# PubNub configuration
config = PNConfiguration()
config.subscribe_key = 'sub-c-83a959c1-2a4f-481b-8eb0-eab514c06ebf'
config.publish_key = 'pub-c-efb82edc-09a0-4fe0-bbf9-29c6b9cbed43'
config.uuid = 'camason' 
pubnub = PubNub(config)

'''    
# code to use without streamz, just pubnub listening and subscribing to a channel
# subscribe to a channel
subscription = pubnub.channel('pubnub-wikipedia').subscription()

# add a listener
subscription.on_message = lambda message: print(f'Message from {message.publisher}: {message.message}')
subscription.subscribe()
'''

# create a stream of wikipedia changes data
source = streamz.Stream()

# create a PubNub callback class to push data into the streamz stream
class MySubscribeCallback(SubscribeCallback):
    def message(self, pubnub, message):
        # convert timetoken from 100-nanosecond intervals since 01/01/1970 to a date time
        epoch_time = int(message.timetoken)/1e7
        timestamp = datetime.fromtimestamp(epoch_time)
        
        # Send the message content to the streamz stream
        source.emit({"message": message.message, "timestamp": timestamp})

# verify data is coming into stream, uncomment to verify
#source.sink(print)
source.map(lambda x: f"Message: {x['message']}, Timestamp: {x['timestamp']}").sink(print)


# query algorithm to track incremental changes as wikipedia pages are changed
def incremental_changes(stream):
    print("entered. incremental changes algorithm")
    # running accumulation of the total number of wikipedia changes, adding one every time a message arrives
    total_changes = stream.accumulate(lambda acc, x: acc + 1, start = 0)
    
    # count changes in different time windows by counting number of messages in each window
    one_minute_window = stream.timed_window(60).map(len)
    one_hour_window = stream.timed_window(60*60).map(len)
    one_day_window = stream.timed_window(60*60*24).map(len)

    # Print the results for debugging
    total_changes.sink(lambda x: print(f"Total Changes: {x}"))
    one_minute_window.sink(lambda x: print(f"Changes in Last Minute: {x}"))
    one_hour_window.sink(lambda x: print(f"Changes in Last Hour: {x}"))
    one_day_window.sink(lambda x: print(f"Changes in Last Day: {x}"))
    
    
# create a thread for the data stream
def data_stream_thread():
    # Instantiate the SubscribeCallback to start listening and pushing data into the streamz stream
    pubnub.add_listener(MySubscribeCallback()) 
    pubnub.subscribe().channels('pubnub-wikipedia').execute()
    
# Createa thread for running the query algorithm
# An asynchronous IO loop to repeatedly call the query algorithm
# at a regular time interval, but does not stop the rest of the program execution 
# like would happen if using time.sleep
'''
async def query_algorithm():
    while True:
        incremental_changes(source)
        await asyncio.sleep(10) #let streaming run for 10 seconds.
'''
# Start thread, data stream running
stream_thread = threading.Thread(target=data_stream_thread)
stream_thread.start()

incremental_changes(source)

# Set the number of columns to display
pd.set_option('display.max_columns', None)

# Create a DataFrame to store timestamps and counts
df = pd.DataFrame(columns=['timestamp', 'count'])

# Streamz window processing
def process_window(window):
    # Record the current count with the most recent timestamp
    if window:
        current_time = datetime.now()
        count = len(window)
        global df
        df = df.append({'timestamp': current_time, 'count': count}, ignore_index=True)
        
        # Keep only the last 24 hours of data
        df = df[df['timestamp'] > current_time - timedelta(hours=24)]
        
        # Plot the data
        plt.figure(figsize=(10, 5))
        plt.plot(df['timestamp'], df['count'], marker='o', linestyle='-')
        plt.xlabel('Time (last 24 hours)')
        plt.ylabel('Number of Wikipedia Changes')
        plt.title('Wikipedia Edits Over the Last 24 Hours')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

# Streamz pipeline to apply the function
one_minute_window = source.timed_window(60).sink(process_window)