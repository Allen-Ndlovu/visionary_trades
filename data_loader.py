import MetaTrader5 as mt5
import pandas as pd
import threading
import websocket
import json
from config import MT5_APP_ID, MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, MT5_SYMBOLS, HISTORICAL_DATA_COUNT, TIMEFRAME

# Global dictionaries to store tick data
latest_ticks = {}
all_ticks = {}

# Initialize tick storage for each volatility symbol (e.g., symbols starting with "R_")
for symbol in MT5_SYMBOLS:
    if symbol.startswith("R_"):
        all_ticks[symbol] = []  # This list will accumulate tick data


# --- MetaTrader 5 Historical Data Functions ---
def connect_mt5():
    if not mt5.initialize():
        print("MT5 initialization failed")
        return False
    authorized = mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
    if not authorized:
        print("MT5 login failed")
        return False
    print("Connected to MetaTrader 5")
    return True


def fetch_mt5_data(symbol, timeframe=mt5.TIMEFRAME_M1, data_count=HISTORICAL_DATA_COUNT):
    """Fetch historical data for a given symbol and timeframe."""
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, data_count)
    if rates is None:
        print("No data fetched for symbol:", symbol)
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    return df


def fetch_all_mt5_data():
    """Fetch data for multiple timeframes from 1-minute to 1-day for all symbols."""
    all_data = {}
    for symbol in MT5_SYMBOLS:
        symbol_data = {}
        for tf, label in TIMEFRAME.items():
            df = fetch_mt5_data(symbol, tf)
            if df is not None:
                symbol_data[label] = df
        all_data[symbol] = symbol_data
    return all_data


# --- Deriv WebSocket (Real-Time Data) Functions ---
def on_message(ws, message):
    try:
        data = json.loads(message)
        if 'tick' in data:
            tick = data['tick']
            symbol = tick['symbol']
            latest_ticks[symbol] = tick
            # Append tick to our storage list
            if symbol in all_ticks:
                all_ticks[symbol].append(tick)
            print(f"Received tick for {symbol}: {tick}")
    except Exception as e:
        print("Error in on_message:", e)


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Closed ###")


def on_open(ws):
    print("WebSocket Connection Established")
    # Subscribe to tick data for every volatility symbol in our symbols list
    for symbol in MT5_SYMBOLS:
        if symbol.startswith("R_"):
            request = {"ticks": symbol}
            ws.send(json.dumps(request))
            print("Subscribed to ticks for:", symbol)


def start_deriv_websocket():
    # Using the app_id from config.py
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={MT5_APP_ID}"  # Dynamically use API key from config
    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever()


def start_deriv_ws_in_thread():
    ws_thread = threading.Thread(target=start_deriv_websocket)
    ws_thread.daemon = True  # so it exits with the main program
    ws_thread.start()


# --- Combined Data Processing Functions ---
def process_combined_data():
    """Process combined data from both historical data (MT5) and real-time ticks (Deriv)."""
    all_data = fetch_all_mt5_data()  # Get historical data for all timeframes and symbols

    # Combine real-time tick data with historical data
    for symbol in all_ticks:
        if symbol in all_data:
            for timeframe, df in all_data[symbol].items():
                # Process both historical data and real-time data here, e.g., combining them or applying strategies
                print(f"Processing data for {symbol} at timeframe {timeframe}")
                print(f"Historical data length: {len(df)}")
                print(f"Real-time tick data length: {len(all_ticks[symbol])}")

                # Example of combining the data for analysis
                df_ticks = pd.DataFrame(all_ticks[symbol])
                df_ticks['time'] = pd.to_datetime(df_ticks['time'], unit='s')
                df_ticks.set_index('time', inplace=True)

                combined_df = pd.concat([df, df_ticks], axis=0).sort_index()

                # You can now perform technical analysis or strategy calculations on combined_df
                print(combined_df.tail())


# --- Main Execution ---
if __name__ == "__main__":
    if connect_mt5():
        start_deriv_ws_in_thread()
        process_combined_data()
