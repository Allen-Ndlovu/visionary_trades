# trade_executor.py

import MetaTrader5 as mt5
from config import MT5_LOGIN, MT5_PASSWORD, MT5_SERVER

def connect_mt5():
    if not mt5.initialize():
        print("MT5 initialization failed")
        return False
    authorized = mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
    if not authorized:
        print("MT5 login failed")
        return False
    return True

def place_trade(symbol, action, lot_size=0.1):
    if not connect_mt5():
        return
    # Get the current price tick
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print("No tick data for symbol:", symbol)
        return
    price = tick.ask if action.lower() == 'buy' else tick.bid
    order_type = mt5.ORDER_TYPE_BUY if action.lower() == 'buy' else mt5.ORDER_TYPE_SELL

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lot_size,
        "type": order_type,
        "price": price,
        "deviation": 10,
        "magic": 234000,
        "comment": "Trend detection trade",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"Trade executed: {action} {symbol} at {price}")
    else:
        print("Trade execution failed:", result)
