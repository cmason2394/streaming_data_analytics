# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 13:23:25 2024

@author: cassi
"""

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error



def load_data_from_csv(file_path):
    # Load the entire CSV into a DataFrame
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        print(f"File {file_path} does not exist.")
        return pd.DataFrame()  # Return an empty DataFrame if the file doesn't exist

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

    # Assume relevant columns for features and target
    X = data[['symbol', 'order_quantity', 'trade_type', 'timestamp', 'bid_price_lag_1', 'bid_price_lag_5']]  # feature columns, what could be influencing price
    y = data['bid-price']  # target column, what is being predicted

    # Train-test split
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Evaluate the model
    predictions = model.predict(X_val)
    mse = mean_squared_error(y_val, predictions)
    print(f"Mean Squared Error: {mse}")

    # Return the trained model
    return model
