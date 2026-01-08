import requests
import sys
import yaml
from typing import Set, List


BINANCE_REST = "https://api.binance.com"


def get_trading_spot_symbols(session: requests.Session) -> Set[str]:
    url = f"{BINANCE_REST}/api/v3/exchangeInfo"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()

    out: Set[str] = set()
    for s in payload.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("isSpotTradingAllowed") is not True:
            continue

        base = s.get("baseAsset")
        quote = s.get("quoteAsset")
        sym = s.get("symbol")

        # 🔒 règles STRICTES
        if not isinstance(sym, str):
            continue
        if not sym.isascii():
            continue
        if not sym.isupper():
            continue
        if not base or not quote:
            continue
        if base == quote:
            continue

        out.add(sym)

    return out


def get_top_pairs(top_n: int = 50) -> List[str]:
    """
    Retrieves the top N pairs from the last 24 hours (by 'count'),
    filtered to Spot symbols that are actively TRADING.
    """
    with requests.Session() as session:
        # 1) Build the allowlist from exchangeInfo
        trading_spot = get_trading_spot_symbols(session)

        # 2) Retrieve 24h ticker for all symbols
        url = f"{BINANCE_REST}/api/v3/ticker/24hr"
        r = session.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        # 3) Filter out non-TRADING Spot symbols (and also keep count > 0)
        filtered = []
        for x in data:
            sym = x.get("symbol")
            if sym not in trading_spot:
                continue
            # Optional safety: ignore fully inactive tickers
            try:
                if int(x.get("count", 0)) <= 0:
                    continue
            except (TypeError, ValueError):
                continue
            filtered.append(x)

        # 4) Sort by number of trades in the last 24h (count), desc
        sorted_pairs = sorted(filtered, key=lambda x: int(x["count"]), reverse=True)

        # 5) Return top N symbols
        top_n_pairs = sorted_pairs[:top_n]
        return [pair["symbol"] for pair in top_n_pairs]


def display_top_pairs_yaml(top_pairs: List[str]) -> str:
    output = {"symbols": top_pairs}
    return yaml.dump(output, sort_keys=False)


if __name__ == "__main__":
    try:
        top_n = int(sys.argv[1]) if len(sys.argv) > 1 else 50
        symbols = get_top_pairs(top_n)
        print(display_top_pairs_yaml(symbols))
    except ValueError:
        print("Usage: python script.py [top_n]")
        sys.exit(1)
    except requests.RequestException as e:
        print(f"HTTP error: {e}")
        sys.exit(2)