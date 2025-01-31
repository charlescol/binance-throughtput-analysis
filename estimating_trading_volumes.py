import requests # type: ignore
from collections import defaultdict, deque
import concurrent.futures
from tqdm import tqdm # type: ignore
import json 
import csv

def api_get_call(url, headers = {}, querystring = {}):
    """
    Sends a request to the Binance API and returns the response.
    """
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status() 
        return response.json()

    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Request Error: {err}")
        return None

def get_exchange_info_symbols():
    """
    Return the *full* array of symbols from the exchangeInfo endpoint.
    That means each element has {baseAsset, quoteAsset, symbol, ...}
    """
    exchange_info = api_get_call("https://api.binance.com/api/v3/exchangeInfo")
    # Return the raw "symbols" list
    return exchange_info["symbols"]

def extract_symbols_ticker(exchange_info_symbols):
    """
    Extract symbols and their quote asset from the exchange info.    
    """
    return {s["symbol"]: s["quoteAsset"] for s in exchange_info_symbols}

def save_json(data, filename):
    """
    Saves data as a JSON file.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"✅ Data saved in {filename}")
        
    except Exception as e:
        print(f"⚠️ Error saving file: {e}")

def save_csv(data, filename):
    """
    Saves data to a CSV file.
    """
    try:
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Symbol", "Quote Asset", "Quote Price in USDT", "24h Quote Volume", "24h Volume in USD"])
            
            for symbol, values in data.items():
                writer.writerow([symbol, values["quote_asset"], values["quote_price"], values["quote_volume"], values["volume_usd"]])
        
        print(f"✅ CSV saved as {filename}")
    except Exception as e:
        print(f"⚠️ Error saving CSV file: {e}")



def fetch_symbol_price(symbol_dict):
    """
    Fetches the price for one symbol (base->quote).
    
    Returns:
        (base, quote, price) or None if request fails.
    """
    base = symbol_dict["baseAsset"]
    quote = symbol_dict["quoteAsset"]
    symbol_name = symbol_dict["symbol"]

    try:
        ticker_data = api_get_call(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol_name}")
        if ticker_data:
            try:
                price = float(ticker_data["price"])
            except (KeyError, ValueError, TypeError):
                price = 0.0
            return (base, quote, price)
    except requests.exceptions.RequestException:
        pass

    return (base, quote, 0.0)

def build_graph(full_symbols_list):
    """
    Builds a graph from the full exchange info list of symbol dicts.
    graph[baseAsset][quoteAsset] = price
    graph[quoteAsset][baseAsset] = 1/price
    """
    graph = defaultdict(dict)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(fetch_symbol_price, s): s
            for s in full_symbols_list
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_symbol), total=len(full_symbols_list), desc="Building graph"):
            symbol_dict = future_to_symbol[future]
            base = symbol_dict["baseAsset"]
            quote = symbol_dict["quoteAsset"]

            try:
                base_fetched, quote_fetched, price = future.result()
                graph[base_fetched][quote_fetched] = price
                if price != 0:
                    graph[quote_fetched][base_fetched] = 1.0 / price

            except Exception as e:
                print(f"⚠️ Error processing symbol {symbol_dict['symbol']}: {e}")
                graph[base][quote] = 0.0  # default

    return graph

def load_graph(filename):
    """
    Loads the trading graph from a JSON file.
    Returns a defaultdict(dict) graph structure.
    """
    try:
        with open(filename, 'r') as f:
            data = json.load(f)

        graph = defaultdict(dict, {k: dict(v) for k, v in data.items()})

        print(f"✅ Graph loaded from {filename}")
        return graph
    except Exception as e:
        print(f"⚠️ Error loading graph file: {e}")
        return defaultdict(dict) 

def bfs_conversion(graph, start_asset, end_asset="USDT"):
    """
    Find any path from start_asset -> end_asset in the graph,
    multiplying along the way. If no path, return 0.
    """
    if start_asset == end_asset:
        return 1.0
    
    visited = set()
    queue = deque([(start_asset, 1.0)]) 

    while queue:
        current_asset, rate_so_far = queue.popleft()
        if current_asset == end_asset:
            return rate_so_far
        if current_asset in visited:
            continue
        visited.add(current_asset)

        # Explore neighbors
        for neighbor, edge_rate in graph[current_asset].items():
            if edge_rate and neighbor not in visited:
                # Multiply the current rate by the edge rate
                queue.append((neighbor, rate_so_far * edge_rate))

    # If BFS fails, no path to USDT
    return 0.0

def get_usdt_price_for_assets(assets, graph):
    """
    For each asset in 'assets', do a BFS to get its price in USDT.
    """
    prices = {}
    for asset in set(assets):
        prices[asset] = bfs_conversion(graph, asset, "USDT")
    return prices

def fetch_symbol_data(symbol, quote_asset, quotes_usdt_prices):
    """
    Fetch and process the symbol data (quoteVolume, volume in USD, etc.).
    """
    try:
        ticker_data = api_get_call(f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}")
        if ticker_data:
            quote_volume = float(ticker_data.get("quoteVolume", 0))
            volume_in_usd = quote_volume * quotes_usdt_prices.get(quote_asset, 0)
            
            return {
                "quote_asset": quote_asset,
                "quote_price": quotes_usdt_prices.get(quote_asset, 0),
                "quote_volume": quote_volume,
                "volume_usd": volume_in_usd
            }
    except requests.exceptions.RequestException:
        print(f"⚠️ Error fetching ticker data for {symbol}. Skipping.")
    
    return {
        "quote_asset": quote_asset,
        "quote_price": 0,
        "quote_volume": 0,
        "volume_usd": 0
    }

def get_symbol_volume(symbols, quotes_usdt_prices): 
    """
    Fetches the volume for each trading symbol in parallel.
    """
    symbol_data = {}

    # If max_workers omitted, Python uses a reasonable default (often # of CPUs * 5).
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(fetch_symbol_data, symbol, quote_asset, quotes_usdt_prices): symbol
            for symbol, quote_asset in symbols.items()
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_symbol), total=len(symbols), desc="Fetching symbols"):
            symbol = future_to_symbol[future]
            try:
                symbol_data[symbol] = future.result()
            except Exception as e:
                print(f"⚠️ An error occurred for symbol {symbol}: {e}")
                symbol_data[symbol] = {
                    "quote_asset": symbols[symbol],
                    "quote_price": 0,
                    "quote_volume": 0,
                    "volume_usd": 0
                }
    return symbol_data

if __name__ == "__main__":
    print("🔄 Fetching symbols...")
    exchange_info = get_exchange_info_symbols()

    symbols = extract_symbols_ticker(exchange_info)
    save_json(symbols, "resources/symbols.json")

    print("🔄 Building the conversion graph...")
    graph = build_graph(exchange_info)
    save_json(graph, "resources/graph.json")
    
    print("🔄 Loading the graph...")
    graph = load_graph("resources/graph.json")

    print("🔄 Calculating quote USD price from the graph...")
    quotes_usdt_prices = get_usdt_price_for_assets(symbols.values(), graph)
    save_json(quotes_usdt_prices, "resources/quotes_usdt_prices.json")

    print("🔄 Calculating symbol volumes in USD...")
    symbol_volumes = get_symbol_volume(symbols, quotes_usdt_prices)
    save_json(symbol_volumes, "resources/symbol_volumes.json")

    print("🔄 Saving results to CSV...")
    save_csv(symbol_volumes, "resources/trading_volumes.csv")

    print("✅ Process complete!")