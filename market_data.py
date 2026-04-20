import json
import zmq
from alpaca.data.live import StockDataStream
import os
from dotenv import load_dotenv

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_PAPER_SECRET_KEY")

ZMQ_PUB_PORT = 5555
"""
NVDA
TSLA
INTC
AMD
AAPL
MSFT
PLTR
AMZN
NFLX
SNDK

"""

SYMBOLS = ["NIO", "SMCI", "SHOP", "AMD", "SNDK", "AAPL", "AMZN", "NFLX", "TSLA", "PLTR"]

def main():
    print(f"Starting Ingestion Node. Binding ZMQ publisher to port {ZMQ_PUB_PORT}...")

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("ERROR: Missing Alpaca API credentials in environment.")
        return

    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    
    publisher.bind(f"tcp://*:{ZMQ_PUB_PORT}")

    print("Initializing Alpaca WebSocket connection...")
    stream = StockDataStream(
        ALPACA_API_KEY, 
        ALPACA_SECRET_KEY, 
    )  

    async def trade_handler(data):
        symbol = data.symbol

        payload = {
            "event": "trade",
            "price": float(data.price),
            "size": float(data.size),
            "timestamp": str(data.timestamp)
        }

        message = f"{symbol} {json.dumps(payload)}"

        publisher.send_string(message)
        
        print(f"Broadcasted tick: {message}")

    stream.subscribe_trades(trade_handler, *SYMBOLS)
    
    print(f"Subscribed to {SYMBOLS}. Listening for live trade tick data:")
    
    try:
        stream.run()
    except KeyboardInterrupt:
        print("\nShutting down ingestion node...")
    finally:
        publisher.close()
        context.term()

if __name__ == "__main__":
    main()

