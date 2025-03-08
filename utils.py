# utils.py (or add to main.py if preferred)
import pandas as pd

def process_tick_data(tick_list, frequency='1min'):
    """
    Convert a list of tick dictionaries into OHLC data.
    Assumes each tick dict contains 'epoch' and 'quote'.
    """
    if not tick_list:
        return None
    df = pd.DataFrame(tick_list)
    # Convert epoch to datetime
    df['time'] = pd.to_datetime(df['epoch'], unit='s')
    df.set_index('time', inplace=True)
    # Resample tick data into OHLC bars based on the 'quote' price
    ohlc = df['quote'].resample(frequency).ohlc()
    return ohlc.reset_index()
