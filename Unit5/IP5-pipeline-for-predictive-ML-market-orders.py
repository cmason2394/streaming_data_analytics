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
import time
import os

import streamz
import pandas as pd
import plotly.graph_objects as go

from sklearn.model_selection import cross_val_predict
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
   

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
agg_df = pd.DataFrame(columns=['total_orders', 'max_price', 'min_price', 'mean_price'])

# Set the number of columns to display
pd.set_option('display.max_columns', None)

# initialize Plotly figure
fig = go.Figure()

# path to CSV file for training ML model
csv_file_path = 'training_data.csv'
agg_data_file_path = 'agg_data.csv'

# function that initializes figure with data
def initialize_plot(symbols):
    global fig
    for symbol in symbols:
        fig.add_trace(go.Scatter( x=[], y=[], mode='lines_markers', name=symbol))
    
    fig.update_layout(
        title='Real-Time Mean Price by Company',
        xaxis_title='Time',
        yaxis_title="Mean Price",
        template='plotly_white'
        )
    
    fig.show()

# function to update the plot as new aggregate data comes in
def update_plot(agg_data):
    # Get the current timestamp
    current_time = agg_data['timestamp'].iloc[0]
    
    # Loop over each symbol in the latest aggregation and update the trace
    for symbol in agg_data['symbol'].unique():
        mean_price = agg_data.loc[agg_data['symbol'] == symbol, 'mean_price'].values[0]
        
        # Locate the trace for the specific symbol by matching name
        for trace in fig.data:
            if trace.name == symbol:
                # Append new x and y values
                trace.x = list(trace.x) + [current_time]
                trace.y = list(trace.y) + [mean_price]
            
            # refresh figure
            fig.show()

# function to process streaming data to make predictions on order prices
def process_data(data):
    global recent_data
    new_row = pd.DataFrame([data])
    
    # Append new row
    recent_data = pd.concat([recent_data, new_row], ignore_index=True)
    #print('recent_data before adding and encoding columns: ', recent_data.columns)
    
    if len(recent_data) > 5:  # Check we have enough rows for lagged features
        recent_data['bid_price_lag_1'] = recent_data['bid_price'].shift(1)
        recent_data['bid_price_lag_5'] = recent_data['bid_price'].shift(5)
        recent_data = recent_data.dropna()
        #print('Columns in recent_data after adding lagged features:', recent_data.columns)
        
        # store bid price and timestamp for future use
        target = recent_data['bid_price']
        timestamps = recent_data['timestamp']
        
        # one-hot encode categorical features to match columns in training data
        recent_data_encoded = pd.get_dummies(recent_data.drop(columns=['bid_price', 'timestamp']), # remove target variable, bid_price, and the timestamp as it is not in a numerical format
                                             columns=['symbol', 'trade_type'], drop_first=True)
        recent_data_encoded = recent_data_encoded.reindex(columns=X.columns, fill_value=0) # align columns with training data, fill in '0' where 'NaN'
        print("Columns in recent_data_encoded:", recent_data_encoded.columns)

        # Use the trained model to predict on the latest data point
        latest_features = recent_data_encoded.iloc[-1:] # most recent row
        predicted_bid_price = trained_model.predict(latest_features)[0]
        
        return {'prediction': predicted_bid_price, 'timestamp': datetime.now()}
    else:
        print("Insufficient data for making predictions.")

# function to append data to a csv file for model training
def append_to_csv(data, file_path):
    # Append `data` to `file_path`. 
    #If file doesn't exist, it creates it with headers (the "not" logic in the header section).
    data.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))
    print(f"Appended {len(data)} rows to {file_path}")

# function to load data stored in a csv file
def load_data_from_csv(file_path):
    # Load the entire CSV into a DataFrame
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        print(f"File {file_path} does not exist.")
        return pd.DataFrame()  # Return an empty DataFrame if the file doesn't exist

# function to train a linear regression model with cross validation and assess the accuracy and variance
def train_model_from_csv(file_path):
    # Load data from CSV
    data = load_data_from_csv(file_path)

    # Check if the DataFrame is empty
    if data.empty:
        print("No data available for training.")
        return
    
    # create lagged features to predict bid price
    data['bid_price_lag_1'] = data['bid_price'].shift(1)
    data['bid_price_lag_5'] = data['bid_price'].shift(5)
    
    # remove NaN values
    data = data.dropna()
    
    # define categorical columns for one-hot encoding
    categorical_columns = ['symbol', 'trade_type']
    
    # one-hot encode categorical features so the model can determine influence of categorical data on bid price
    # adds columns to df where each category is 1 or 0. Drop_first removes first column because when other 
    #columns are all 0, implies 1st column is 1
    data = pd.get_dummies(data, columns=categorical_columns, drop_first=True) 

    # Assume relevant columns for features and target
    X = data.drop(columns = ['bid_price', 'timestamp'])  # feature columns, what could be influencing price.
    y = data['bid_price']  # target column, what is being predicted

    # Initialize the model
    model = LinearRegression()
    
    # run cross validation to get estimate on accuracy and variance of predictions
    estimates = cross_val_predict(model, X, y)
    
    # calculate metrics, how accurate and variable model is
    mse = mean_squared_error(y, estimates)
    r2 = r2_score(y, estimates)

    print("Cross-Validation MSE:", mse)
    print("Cross-Validation R²:", r2)
    
    # Fit the model with the entire dataset
    model.fit(X, y)
    print("Model trained on entire dataset.")
    
    return model, X

# function to print out linear equation
def print_model_equation(model, feature_names):
    intercept = model.intercept_
    coefficients = model.coef_
    
    equation = f"bid_price = {intercept:.2f}"
    for feature, coef in zip(feature_names, coefficients):
        equation += f" + ({coef:.2f} * {feature})"
    
    print("Model Equation:")
    print(equation)

# call training function
trained_model, X = train_model_from_csv(csv_file_path)   

        
# Function to perform aggregation and clearing of old data, triggered every hour
def aggregate_and_save():
    print('entered aggregate function')
    global recent_data, agg_df
    
    # Initialize plot with unique symbols from recent_data
    symbols = recent_data['symbol'].unique() if not recent_data.empty else []
    initialize_plot(symbols)
    
    while True:
            time.sleep(60) # run every minute
            print('after sleeping for a minute')
            
            # append last minute of data to a CSV file
            #append_to_csv(recent_data, csv_file_path)
            
            # Perform aggregation: total orders made in the last minute, minimum, maximum and mean of orders in last minute
            if not recent_data.empty and 'symbol' in recent_data.columns:
                agg_data = recent_data.groupby('symbol').agg(
                    total_orders=('order_quantity', 'sum'),
                    max_price=('bid_price', 'max'),
                    min_price=('bid_price', 'min'),
                    mean_price=('bid_price', 'mean')
                ).reset_index()
                
                # include current timestamp in each row 
                agg_data['timestamp'] = datetime.now()
                
                # append aggregate data to a CSV file
                append_to_csv(agg_data, agg_data_file_path)
            
                #print("Aggregated Data:", agg_data)
                print('did aggregation')
                
                # Update plot with new data
                update_plot(agg_data)
                
                # store aggregates in long-term storage
                agg_df = pd.concat([agg_df, agg_data], ignore_index=True)
                print('updated agg_df: ', agg_df)
         
                # Reset recent_data for the next minute's window
                recent_data = pd.DataFrame()
            else:
                print("No data to aggregate this minute.")


# No-op function to use as a sink, so data flows through the stream without printing
def noop(data):
    pass

# create a PubNub callback class to push data into the streamz stream
class MySubscribeCallback(SubscribeCallback):
    def message(self, pubnub, message):
        # convert timetoken from 100-nanosecond intervals since 01/01/1970 to a date time
        epoch_time = int(message.timetoken)/1e7
        readable_timestamp = datetime.fromtimestamp(epoch_time)
        
        # Add converted timestamp directly to the message dictionary
        message.message['timestamp'] = readable_timestamp
        
        # Ensure all required fields are included
        if 'symbol' in message.message and 'order_quantity' in message.message and 'bid_price' in message.message:
            # send message to Streamz stream
            source.emit(message.message)
        else:
            print("Message missing required fields:", message.message)

# verify data is coming into stream, uncomment to verify
#source.sink(print)
#source.map(lambda x: f"Message: {x['message']}, Timestamp: {x['timestamp']}").sink(print)

# Use Streamz to process the data and predict the bid price of streaming orders
source.map(process_data).sink(lambda result: print(result['prediction']) if result else None)

# create a thread for the data stream
def data_stream_thread():
    # Instantiate the SubscribeCallback to start listening and pushing data into the streamz stream
    pubnub.add_listener(MySubscribeCallback()) 
    pubnub.subscribe().channels('pubnub-market-orders').execute()
    
# Start data stream thread
stream_thread = threading.Thread(target=data_stream_thread)
stream_thread.start()

aggregate_and_save()


