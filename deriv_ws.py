import websocket
import json


def on_message(ws, message):
    try:
        data = json.loads(message)
        print("Received Message:", data)
    except Exception as e:
        print("Error parsing message:", e)


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Closed ###")


def on_open(ws):
    print("WebSocket Connection Established")
    # Subscribe to tick data for Volatility 75 Index (symbol R_75)
    subscription_request = {
        "ticks": "R_75"
    }
    ws.send(json.dumps(subscription_request))
    print("Subscription Request Sent:", subscription_request)


if __name__ == "__main__":
    # Use the demo endpoint for Binary/Deriv API
    ws_url = "wss://ws.binaryws.com/websockets/v3?app_id=1089"

    # If you're using a live account, you might try:
    # ws_url = "wss://ws.deriv.com/websockets/v3"

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws_app.run_forever()
