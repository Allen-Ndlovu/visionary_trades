import pandas as pd
import numpy as np
import talib


def calculate_rsi(df, period=14):
    """Calculate Relative Strength Index (RSI) using Talib for a DataFrame."""
    rsi = talib.RSI(df['close'], timeperiod=period)
    return rsi

def calculate_macd(df, short_period=12, long_period=26, signal_period=9):
    """Calculate MACD (MACD minus its signal line) using Talib for a DataFrame."""
    macd, macd_signal, _ = talib.MACD(df['close'], fastperiod=short_period, slowperiod=long_period, signalperiod=signal_period)
    return macd - macd_signal

def calculate_sma(df, period=20):
    """Calculate Simple Moving Average (SMA) for a DataFrame."""
    sma = talib.SMA(df['close'], timeperiod=period)
    return sma

def calculate_ema(df, period=20):
    """Calculate Exponential Moving Average (EMA) for a DataFrame."""
    ema = talib.EMA(df['close'], timeperiod=period)
    return ema


def identify_candlestick_patterns(df):
    """
    Identifies basic candlestick patterns: Doji, Engulfing, and Pin Bar.
    """
    df['Body'] = abs(df['close'] - df['open'])
    df['UpperShadow'] = df['high'] - df[['close', 'open']].max(axis=1)
    df['LowerShadow'] = df[['close', 'open']].min(axis=1) - df['low']

    # Doji: small body relative to overall range
    df['Doji'] = (df['Body'] < (df['high'] - df['low']) * 0.1)

    # Bullish Engulfing
    df['Bullish_Engulfing'] = (
            (df['close'].shift(1) < df['open'].shift(1)) &
            (df['close'] > df['open']) &
            (df['close'] > df['open'].shift(1)) &
            (df['open'] < df['close'].shift(1))
    )

    # Bearish Engulfing
    df['Bearish_Engulfing'] = (
            (df['close'].shift(1) > df['open'].shift(1)) &
            (df['close'] < df['open']) &
            (df['open'] > df['close'].shift(1)) &
            (df['close'] < df['open'].shift(1))
    )

    # Pin Bar patterns
    df['Bullish_Pin_Bar'] = (df['LowerShadow'] > df['Body'] * 2) & (df['UpperShadow'] < df['Body'])
    df['Bearish_Pin_Bar'] = (df['UpperShadow'] > df['Body'] * 2) & (df['LowerShadow'] < df['Body'])

    return df

def generate_weighted_signals(df_indicators):
    """
    Generates trading signals (Buy/Sell) based on weighted indicators (RSI, MACD, SMA, EMA).
    If any indicator is missing from the DataFrame, it is calculated.
    :param df_indicators: DataFrame containing OHLC data.
    :return: DataFrame with additional columns for individual signals, weighted signal, and final signal.
    """
    # Calculate missing indicators if needed
    if 'RSI' not in df_indicators.columns:
        df_indicators['RSI'] = calculate_rsi(df_indicators)
    if 'MACD' not in df_indicators.columns:
        df_indicators['MACD'] = calculate_macd(df_indicators)
    if 'SMA' not in df_indicators.columns:
        df_indicators['SMA'] = calculate_sma(df_indicators)
    if 'EMA' not in df_indicators.columns:
        df_indicators['EMA'] = calculate_ema(df_indicators)

    # Weight factors for each indicator
    indicator_weights = {
        'RSI': 0.4,
        'MACD': 0.3,
        'SMA': 0.2,
        'EMA': 0.1,
    }

    # Ensure all required indicators exist in df_indicators
    for indicator in indicator_weights.keys():
        if indicator not in df_indicators.columns:
            raise ValueError(f"Indicator column '{indicator}' is missing from the dataframe.")

    # Calculate individual signals based on thresholds
    df_indicators['RSI_signal'] = np.where(df_indicators['RSI'] > 70, 'Sell',
                                           np.where(df_indicators['RSI'] < 30, 'Buy', 'Neutral'))
    df_indicators['MACD_signal'] = np.where(df_indicators['MACD'] > 0, 'Buy',
                                            np.where(df_indicators['MACD'] < 0, 'Sell', 'Neutral'))
    df_indicators['SMA_signal'] = np.where(df_indicators['SMA'] > df_indicators['close'], 'Buy', 'Sell')
    df_indicators['EMA_signal'] = np.where(df_indicators['EMA'] > df_indicators['close'], 'Buy', 'Sell')

    # Calculate the weighted signal
    df_indicators['weighted_signal'] = (
        df_indicators['RSI_signal'].map({'Buy': 1, 'Sell': -1, 'Neutral': 0}) * indicator_weights['RSI'] +
        df_indicators['MACD_signal'].map({'Buy': 1, 'Sell': -1, 'Neutral': 0}) * indicator_weights['MACD'] +
        df_indicators['SMA_signal'].map({'Buy': 1, 'Sell': -1}) * indicator_weights['SMA'] +
        df_indicators['EMA_signal'].map({'Buy': 1, 'Sell': -1}) * indicator_weights['EMA']
    )

    # Determine the final signal based on the weighted score
    df_indicators['final_signal'] = np.where(df_indicators['weighted_signal'] > 0, 'Buy',
                                             np.where(df_indicators['weighted_signal'] < 0, 'Sell', 'Hold'))
    return df_indicators

def generate_signals(df):
    """
    Generates buy/sell signals based on candlestick patterns and weighted technical indicators
    using a weighted majority algorithm. Provides reasoning for signals.
    :param df: DataFrame with OHLC data and 'close' column.
    :return: DataFrame with final signals, including time, final_signal, reasoning, and Symbol.
    """
    # Convert 'time' to datetime and set as index
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    df.set_index('time', inplace=True)

    if df.empty:
        print("Error: DataFrame is empty")
        return pd.DataFrame()

    # Define timeframes and weights for the multi-timeframe approach
    timeframes = ['1min', '5min', '15min', '30min', '1h', '4h', '1d']
    weights = {
        '1min': 1,
        '5min': 1,
        '15min': 2,
        '30min': 3,
        '1h': 4,
        '4h': 5,
        '1d': 6
    }

    all_signals = []  # To store signals for each timeframe

    # Generate signals for each timeframe
    for timeframe in timeframes:
        df_resampled = df.resample(timeframe).agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})
        df_resampled = identify_candlestick_patterns(df_resampled)
        # Generate weighted technical indicator signals
        df_resampled = generate_weighted_signals(df_resampled)

        timeframe_signals = []
        for i in range(1, len(df_resampled)):
            signal = None
            current_time = df_resampled.index[i]
            # Candlestick pattern signal
            if df_resampled['Bullish_Engulfing'].iloc[i] or df_resampled['Bullish_Pin_Bar'].iloc[i]:
                signal = 'Buy'
            elif df_resampled['Bearish_Engulfing'].iloc[i] or df_resampled['Bearish_Pin_Bar'].iloc[i]:
                signal = 'Sell'

            # Incorporate weighted technical indicator signal
            if df_resampled['final_signal'].iloc[i] == 'Buy':
                signal = 'Buy' if signal != 'Sell' else 'Buy'
            elif df_resampled['final_signal'].iloc[i] == 'Sell':
                signal = 'Sell' if signal != 'Buy' else 'Sell'

            timeframe_signals.append({'time': current_time, 'Signal': signal if signal else 'Sell'})  # Default to Sell if no signal
        all_signals.append(timeframe_signals)

    # Combine signals from all timeframes using a weighted majority vote
    final_signals = []
    for index in range(len(df_resampled)):
        weighted_sum = 0
        reasoning = []
        for tf_idx, timeframe_signals in enumerate(all_signals):
            if index - 1 >= 0 and index - 1 < len(timeframe_signals):
                sig = timeframe_signals[index - 1]['Signal']
            else:
                sig = 'Sell'  # Default to Sell if no signal

            if sig == 'Buy':
                weighted_sum += weights[timeframes[tf_idx]]
                reasoning.append(f"{timeframes[tf_idx]}: Buy signal")
            elif sig == 'Sell':
                weighted_sum -= weights[timeframes[tf_idx]]
                reasoning.append(f"{timeframes[tf_idx]}: Sell signal")

        if weighted_sum > 0:
            final_signal = 'Buy'
            final_reasoning = "Strong entry from 1-min and 5-min signals, supported by trend strength from higher timeframes."
        else:
            final_signal = 'Sell'
            final_reasoning = "Strong entry from 1-min and 5-min signals, supported by trend strength from higher timeframes."

        final_signals.append({
            'time': df_resampled.index[index],
            'final_signal': final_signal,
            'reasoning': final_reasoning
        })

    final_df = pd.DataFrame(final_signals)
    # Propagate Symbol if available
    if 'Symbol' in df.columns:
        final_df['Symbol'] = df['Symbol'].iloc[0]
    else:
        final_df['Symbol'] = 'Unknown'
    return final_df
