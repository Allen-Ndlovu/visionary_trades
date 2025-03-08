import MetaTrader5 as mt5
import pandas as pd
import threading
import websocket
import json
import time
from config import MT5_APP_ID, MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, MT5_SYMBOLS, HISTORICAL_DATA_COUNT, TIMEFRAME
from data_loader import start_deriv_ws_in_thread, all_ticks
from indicators import calculate_indicators
from signal_generator import generate_signals  # Modified signal generation function (weighted algorithm)
from trade_executor import place_trade
from utils import process_tick_data

# Global dictionaries to store tick data
latest_ticks = {}
all_ticks = {}

# Initialize tick storage for each volatility symbol (e.g., symbols starting with "R_")
for symbol in MT5_SYMBOLS:
    if symbol.startswith("R_"):
        all_ticks[symbol] = []  # This list will accumulate tick data

# --- MetaTrader 5 Historical Data Functions ---
def connect_mt5():
    """Initialize the MT5 connection."""
    if not mt5.initialize():
        print("MT5 initialization failed")
        return False
    authorized = mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
    if not authorized:
        print("MT5 login failed")
        return False
    print("Connected to MetaTrader 5")
    return True

def fetch_mt5_data(symbol):
    """Fetch historical data from MT5 for a given symbol."""
    timeframe = mt5.TIMEFRAME_M1  # 1-minute timeframe
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, HISTORICAL_DATA_COUNT)
    if rates is None:
        print(f"No data fetched for symbol: {symbol}")
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

# --- Deriv WebSocket (Real-Time Data) Functions ---
def on_message(ws, message):
    """Handle incoming messages from the WebSocket."""
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
    """Handle WebSocket errors."""
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket closure."""
    print("### WebSocket Closed ###")

def on_open(ws):
    """Handle WebSocket connection establishment."""
    print("WebSocket Connection Established")
    # Subscribe to tick data for every volatility symbol in our symbols list
    for symbol in MT5_SYMBOLS:
        if symbol.startswith("R_"):
            request = {"ticks": symbol}
            ws.send(json.dumps(request))
            print(f"Subscribed to ticks for: {symbol}")

def start_deriv_websocket():
    """Start the WebSocket connection."""
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
    """Start Deriv WebSocket in a separate thread."""
    ws_thread = threading.Thread(target=start_deriv_websocket)
    ws_thread.daemon = True  # So it exits with the main program
    ws_thread.start()

def main():
    """Main function to process real-time tick data from Deriv API and generate signals."""
    # Connect to MT5
    if not connect_mt5():
        print("Unable to connect to MetaTrader 5, exiting...")
        return

    # Start real-time data feed via Deriv WebSocket (runs in background)
    start_deriv_ws_in_thread()

    print("Collecting real-time tick data...")

    # Symbol of interest (e.g., VIX75, but it can be changed to any other symbol)
    symbol = "R_75"  # Example symbol: Volatility 75 Index
    while True:
        tick_list = all_ticks.get(symbol, [])
        if len(tick_list) < 5:
            print("Not enough tick data yet, waiting...")
            time.sleep(5)
            continue  # Skip iteration if not enough data

        # Convert tick data to OHLC format using our utility
        ohlc_df = process_tick_data(tick_list, frequency='1min')
        if ohlc_df is not None and not ohlc_df.empty:
            ohlc_df['Symbol'] = symbol  # Add Symbol column
            print("\n--- Processed OHLC Data ---")
            print(ohlc_df.tail())

            # Compute additional indicators (e.g., RSI, MACD, etc.)
            df_indicators = calculate_indicators(ohlc_df.copy())

            # Generate Buy/Sell signals across multiple timeframes (weighted algorithm)
            df_signals = generate_signals(df_indicators.copy())
            # Ensure the 'Symbol' column is carried forward (if not, add it here)
            df_signals['Symbol'] = symbol

            # Filter and display generated signals
            signals = df_signals[df_signals['final_signal'].notnull()]
            if not signals.empty:
                print("\n--- Trading Signals ---")
                for _, row in signals.iterrows():
                    print(f"Time: {row['time']}, Symbol: {row['Symbol']}, Final Signal: {row['final_signal']}")

                # Execute the latest signal
                latest_signal = df_signals.iloc[-1].get('final_signal')
                print(f"\nLatest Signal for {symbol}: {latest_signal}")
                if latest_signal == 'Buy':
                    place_trade(symbol, 'buy')
                elif latest_signal == 'Sell':
                    place_trade(symbol, 'sell')
            else:
                print("No signals generated.")
        else:
            print("No sufficient tick data to generate OHLC.")

        time.sleep(5)  # Delay before processing next batch

if __name__ == "__main__":
    main()
