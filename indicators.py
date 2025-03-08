# indicators.py

import pandas as pd
import talib

def calculate_indicators(df):
    if 'close' not in df.columns:
        print("DataFrame does not contain 'close' column.")
        return df

    df['SMA_50'] = talib.SMA(df['close'], timeperiod=50)
    df['SMA_200'] = talib.SMA(df['close'], timeperiod=200)
    df['EMA_20'] = talib.EMA(df['close'], timeperiod=20)
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    macd, macd_signal, macd_hist = talib.MACD(df['close'])
    df['MACD'] = macd
    df['MACD_signal'] = macd_signal
    return df
