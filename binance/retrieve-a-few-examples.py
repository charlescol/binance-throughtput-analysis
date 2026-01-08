
import asyncio
import websockets
import json

# List to store all received messages
responses = []

async def listen():
    uri = "wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms"
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}. Listening for messages (press Ctrl+C to stop)...")
        while True:
            msg = await websocket.recv()
            responses.append(msg)
            # Optionally print a snippet of the received message
            print(f"Received message: {msg[:80]}...")

def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(listen())
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt detected. Saving responses to file...")
        # Optionally, decode the JSON messages before saving.
        try:
            # Convert each message string to a JSON object
            responses_json = [json.loads(message) for message in responses]
            output_data = responses_json
        except json.JSONDecodeError:
            # If any message fails to decode, save the raw message strings.
            output_data = responses
        
        # Save the collected responses to a file
        with open("resources/example-responses.json", "w") as outfile:
            json.dump(output_data, outfile, indent=2)
        print("Responses saved to responses.json.")
    finally:
        loop.close()

if __name__ == '__main__':
    main()