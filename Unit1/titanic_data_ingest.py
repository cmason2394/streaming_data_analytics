# -*- coding: utf-8 -*-
"""
Created on Fri Oct 11 16:07:59 2024

@author: cassi
"""
print('Spyder and Anaconda successfully downloaded!')

import pandas as pd
import streamz

# Set the number of columns to display
pd.set_option('display.max_columns', None)

# Create a Stream to read the CSV file
stream = streamz.Stream()

# Define a function to read the CSV file
def read_csv():
    df = pd.read_csv("titanic_train.csv")
    print(df.columns)
    print(df.head())
    return df

# Emit the DataFrame from the read_csv function
stream.emit(read_csv())

# Function to clean data
def process_data(df):
    # remove the columns 'cabin' and 'body'
    df = df.drop(columns = ['cabin', 'body'])
    
    # replace NaN with '-1' in 'boat' and 'home.dest' column
    df['boat'] = df['boat'].fillna('-1')
    df['home.dest'] = df['home.dest'].fillna('-1')
    
    # for all other columns, empty cells will delete row (dropna)
    df = df.dropna()
    
    # ensure correct data types: passenger_id, pclass, sibsp, parch are integers
    # ensure correct data types: sex is either female or male, embarked is [Q,S,C], survived is [0,1]
    # ensure correct data types: name and home.dest is string
    # ensure correct data types: fare is float
    
    return df


# Clean the data coming in through the stream
stream.map(process_data(read_csv()))#.sink(print)
#stream.sink(lambda df: print(df.head()))


#stream.emit(process_data(read_csv))
print(process_data(read_csv()))

# report results of the streaming pipeline with a visualization


