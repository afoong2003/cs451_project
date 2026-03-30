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

def main():
    print(f"starting node:  {ZMQ_PULL_PORT}...")
    context = zmq.Context()
    
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://*:{ZMQ_PULL_PORT}")

    print("Starting trade :")
    trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

    print("Waiting for decisions to execute: ")

    try:
        while True:
            message = receiver.recv_string()
            signal = json.loads(message)
            
            symbol = signal.get("symbol")
            action = signal.get("action")  
            qty = signal.get("qty", 1)     
            
            print(f"Received Command: {action} {qty} shares of {symbol}")

            try:
                trading_client.cancel_orders() 
                
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