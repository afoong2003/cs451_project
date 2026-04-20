import asyncio
import json
import zmq
import websockets

ZMQ_PUB_PORT = 5555

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT"]

BINANCE_TESTNET_WS_BASE = "wss://stream.testnet.binance.vision/stream?streams="
RECONNECT_DELAY_SECONDS = 3


def build_stream_url(symbols):
    streams = [f"{symbol.lower()}@trade" for symbol in symbols]
    return BINANCE_TESTNET_WS_BASE + "/".join(streams)


async def stream_trades(publisher):
    ws_url = build_stream_url(SYMBOLS)

    while True:
        try:
            print(f"Connecting to Binance Spot Testnet WebSocket: {ws_url}")
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
                print("Connected to Binance Spot Testnet trade stream.")
                async for raw_message in websocket:
                    message_json = json.loads(raw_message)
                    data = message_json.get("data", {})

                    symbol = data.get("s")
                    price = data.get("p")
                    size = data.get("q")
                    timestamp = data.get("T", data.get("E", "unknown"))

                    if not symbol or price is None or size is None:
                        continue

                    payload = {
                        "event": "trade",
                        "price": float(price),
                        "size": float(size),
                        "timestamp": str(timestamp),
                    }

                    message = f"{symbol} {json.dumps(payload)}"
                    publisher.send_string(message)
                    print(f"Broadcasted tick: {message}")

        except Exception as exc:
            print(
                f"Binance websocket disconnected ({exc}). "
                f"Reconnecting in {RECONNECT_DELAY_SECONDS}s..."
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

def main():
    print(f"Starting Ingestion Node. Binding ZMQ publisher to port {ZMQ_PUB_PORT}...")

    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    
    publisher.bind(f"tcp://*:{ZMQ_PUB_PORT}")

    print(
        "Initializing Binance Spot Testnet WebSocket connection "
    )
    print(f"Subscribed to {SYMBOLS}. Listening for live trade tick data:")
    
    try:
        asyncio.run(stream_trades(publisher))
    except KeyboardInterrupt:
        print("\nShutting down ingestion node...")
    finally:
        publisher.close()
        context.term()

if __name__ == "__main__":
    main()

