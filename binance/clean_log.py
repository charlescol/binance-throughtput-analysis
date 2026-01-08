#!/usr/bin/env python3
import json
from collections import Counter
from pathlib import Path
from typing import Optional, Set, Tuple


MARKER = "WS text frame: "
DECODER = json.JSONDecoder()


def parse_ws_frame_json(line: str) -> Optional[dict]:
    """
    Extract and parse the JSON object right after 'WS text frame: '.
    Works even if trailing text exists (e.g., 'group_id=...').
    """
    idx = line.find(MARKER)
    if idx == -1:
        return None

    s = line[idx + len(MARKER):].lstrip()
    try:
        obj, _end = DECODER.raw_decode(s)
    except json.JSONDecodeError:
        return None

    if isinstance(obj, dict):
        return obj
    return None


def extract_symbols_from_payload(payload: dict) -> Set[str]:
    """
    From payload like {"result":["alpacausdt@depth", ...], "id":9999}
    extract the symbol before '@' (e.g., 'alpacausdt').
    """
    result = payload.get("result")
    if not isinstance(result, list):
        return set()

    out: Set[str] = set()
    for item in result:
        if not isinstance(item, str):
            continue
        symbol = item.split("@", 1)[0].strip()
        if symbol:
            out.add(symbol)
    return out


def scan_file(path: Path) -> Tuple[Set[str], int, int, Counter]:
    """
    Returns:
      - distinct symbols
      - matched lines (WS text frame lines that had a non-empty result list)
      - total symbol occurrences (sum of symbols per matched line)
      - per-symbol occurrences (Counter)
    """
    distinct_symbols: Set[str] = set()
    matched_lines = 0
    occurrences = 0
    per_symbol = Counter()

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            payload = parse_ws_frame_json(line)
            if not payload:
                continue

            symbols = extract_symbols_from_payload(payload)
            if not symbols:
                # Covers result:null and result:[]
                continue

            matched_lines += 1
            occurrences += len(symbols)
            distinct_symbols.update(symbols)
            per_symbol.update(symbols)

    return distinct_symbols, matched_lines, occurrences, per_symbol


def main() -> None:
    path = Path("logs.txt")
    if not path.exists():
        raise SystemExit("Error: logs.txt not found in current directory.")

    distinct, matched_lines, occurrences, per_symbol = scan_file(path)

    print(f"Matched lines (non-empty result list): {matched_lines}")
    print(f"Symbol occurrences (sum per matched line): {occurrences}")
    print(f"Distinct symbols: {len(distinct)}")

    # Optional: print distinct symbols sorted
    # for s in sorted(distinct):
    #     print(s)

    # Optional: show the most frequent symbols
    print("\nTop 20 symbols by occurrence:")
    for sym, cnt in per_symbol.most_common(20):
        print(f"{sym}\t{cnt}")


if __name__ == "__main__":
    main()