import json
import zmq
from alpaca.data.live import StockDataStream
from alpaca.data.live import CryptoDataStream
import os
from dotenv import load_dotenv

load_dotenv()

ALPACA_API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_PAPER_SECRET_KEY")

ZMQ_PUB_PORT = 5555

#SYMBOLS = ["AAPL", "MSFT", "TSLA"]

SYMBOLS = ["FAKEPACA"]

def main():
    print(f"Starting Ingestion Node. Binding ZMQ publisher to port {ZMQ_PUB_PORT}...")
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    
    publisher.bind(f"tcp://*:{ZMQ_PUB_PORT}")

    print("Initializing Alpaca WebSocket connection...")
    stream = StockDataStream(
        ALPACA_API_KEY, 
        ALPACA_SECRET_KEY, 
        url_override="wss://stream.data.alpaca.markets/v2/test"
    )  

    async def trade_handler(data):
       
        symbol = data.symbol

        payload = {
            "price": data.price,
            "size": data.size,
            "timestamp": str(data.timestamp)
        }

        message = f"{symbol} {json.dumps(payload)}"

        publisher.send_string(message)
        
        print(f"Broadcasted: {message}")

    stream.subscribe_trades(trade_handler, *SYMBOLS)
    
    print(f"Subscribed to {SYMBOLS}. Listening for live market data:")
    
    try:
        stream.run()
    except KeyboardInterrupt:
        print("\nShutting down ingestion node...")
    finally:
        publisher.close()
        context.term()

if __name__ == "__main__":
    main()