import os
import json
import zmq
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_PAPER_SECRET_KEY")

ZMQ_PULL_PORT = 5556 


def has_open_position(trading_client, symbol):
    positions = trading_client.get_all_positions()
    for position in positions:
        if position.symbol == symbol and abs(float(position.qty)) > 0:
            return True
    return False

def main():
    context = zmq.Context()
    
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://*:{ZMQ_PULL_PORT}")

    print("Starting trade :")
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

    print("Waiting for decisions to execute: ")

    try:
        while True:
            message = receiver.recv_string()
            try:
                signal = json.loads(message)
                symbol = str(signal.get("symbol", "")).upper()
                action = str(signal.get("action", "")).upper()
                qty = int(signal.get("qty", 1))
            except (json.JSONDecodeError, ValueError, TypeError):
                print(f"ERROR: Invalid signal payload received: {message}")
                continue

            if not symbol or action not in {"BUY", "SELL"} or qty <= 0:
                print(f"ERROR: Invalid trading signal fields: {signal}")
                continue
            
            print(f"Received Command: {action} {qty} shares of {symbol}")

            try:
                if action == "BUY":
                    order_data = MarketOrderRequest(
                        symbol=symbol,
                        qty=qty,
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY
                    )
                    trading_client.submit_order(order_data=order_data)
                    print(f"SUCCESS: Order filled for {symbol}\n")

                elif action == "SELL":
                    position_exists = has_open_position(trading_client, symbol)
                    if not position_exists:
                        print(f"SKIP: No open position for {symbol}; SELL ignored.\n")
                        continue

                    trading_client.close_position(symbol)
                    print(f"SUCCESS: Position closed for {symbol}\n")
                    
            except Exception as e:
                print(f"ERROR: Failed to execute trade for {symbol}. {e}\n")

    except KeyboardInterrupt:
        print("\nShutting down Execution Node...")
    finally:
        receiver.close()
        context.term()

if __name__ == "__main__":
    main()