import asyncio
import json
import time
import websockets  # type: ignore
import pandas as pd  # type: ignore

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


async def subscribe_to_streams(ws, streams, request_id=1):
    subscribe_request = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": request_id
    }
    await ws.send(json.dumps(subscribe_request))
    print(f"[Sent] SUBSCRIBE request ID={request_id}, Count={len(streams)}")
    await asyncio.sleep(1) 

async def unsubscribe_from_streams(ws, streams, request_id=2):
    unsubscribe_request = {
        "method": "UNSUBSCRIBE",
        "params": streams,
        "id": request_id
    }
    await ws.send(json.dumps(unsubscribe_request))
    print(f"[Sent] UNSUBSCRIBE request ID={request_id}, Count={len(streams)}")
    await asyncio.sleep(1)

async def websocket_handler(symbols_slice, name, global_data, global_start_time):
    """
    Handles a single WebSocket connection for the given slice of symbols.
    Subscribes to trade streams, counts messages, prints throughput each second,
    and accumulates that throughput into `global_data`.
    """
    stream_list = [f"{symbol.lower()}@trade" for symbol in symbols_slice]
    print(f"[{name}] Attempting to connect. Symbol count: {len(stream_list)}")

    try:
        async with websockets.connect(BINANCE_WS_URL) as ws:
            await subscribe_to_streams(ws, stream_list[0:300], request_id=1)
            await subscribe_to_streams(ws, stream_list[300:600], request_id=2)
            await subscribe_to_streams(ws, stream_list[600:1000], request_id=3)

            message_count = 0
            local_start_time = time.time()

            while True:
                message = await ws.recv()

                if isinstance(message, bytes): 
                    print(f"[{name}] Received ping frame, responding with pong.")
                    await ws.pong(message)  # Respond with same payload
                    continue

                try:
                    message_count += 1
                except json.JSONDecodeError:
                    print(f"[{name}] Received non-JSON message: {message}")
                    continue

                current_time = time.time()
                if current_time - local_start_time >= 1.0:
                    print(f"[{name}] Messages/second: {message_count}")

                    elapsed_sec = int(current_time - global_start_time)
                    
                    # Aggregate into global_data
                    # We sum across connections by storing them all in one bucket.
                    # Each connection adds its per-second count to the same second key.
                    global_data[elapsed_sec] = global_data.get(elapsed_sec, 0) + message_count

                    message_count = 0
                    local_start_time = current_time

    except asyncio.CancelledError:
        print(f"\n[{name}] Task cancelled. Cleaning up WebSocket...")
        # Attempt graceful unsubscribe
        async with websockets.connect(BINANCE_WS_URL) as ws:
            await unsubscribe_from_streams(ws, stream_list[0:300], request_id=4)
            await unsubscribe_from_streams(ws, stream_list[300:600], request_id=5)
            await unsubscribe_from_streams(ws, stream_list[600:1000], request_id=6)
        print(f"[{name}] WebSocket connection closed.")
        raise  # re-raise so higher-level code knows the task is cancelled

async def main():
    # Read symbols from your file
    df = pd.read_csv("binance/resources/trading_volumes.csv")
    all_symbols = df["Symbol"].tolist()

    # 3 slices of up to 1,000 each
    slice1 = all_symbols[0:1000]
    slice2 = all_symbols[1000:2000]
    slice3 = all_symbols[2000:3000]

    # A shared dictionary to accumulate results across all connections.
    # Key: elapsed second (int), Value: sum of messages from all connections in that second.
    global_data = {}

    # We'll record when all tasks start, to have a common "zero" time reference
    global_start_time = time.time()

    # Create three tasks, each with its own WebSocket connection
    task1 = asyncio.create_task(websocket_handler(slice1, "Connection1", global_data, global_start_time))
    task2 = asyncio.create_task(websocket_handler(slice2, "Connection2", global_data, global_start_time))
    task3 = asyncio.create_task(websocket_handler(slice3, "Connection3", global_data, global_start_time))

    # Run them concurrently until they're done or cancelled
    try:
        await asyncio.gather(task1, task2, task3)
    except asyncio.CancelledError:
        pass  # We'll handle cleanup below
    finally:
        # Once done (or on Ctrl+C), dump global_data to a CSV
        times = sorted(global_data.keys())
        counts = [global_data[t] for t in times]
        output_df = pd.DataFrame({"time_sec": times, "messages_per_second": counts})
        output_df.to_csv("binance/resources/aggregated_results.csv", index=False)
        print("\n[Info] Results saved to aggregated_results.csv")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n[Info] KeyboardInterrupt received. Cancelling tasks...")
        for t in asyncio.all_tasks(loop):
            t.cancel()
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
        print("[Info] Exiting cleanly.")
    finally:
        loop.close()