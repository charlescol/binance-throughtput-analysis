#!/usr/bin/env python3
"""
orderbook_local.py

Quick, dependency-free script to:
  1) Load an initial order book snapshot from a JSON file.
  2) Apply a series of incremental depth updates (e.g., Binance-style) from a JSON file.
  3) Store **in memory** the resulting book after each update (or every Nth update),
     and optionally dump the collection to a JSON file.

No databases, no production frills.

Input formats
-------------
Snapshot file (either a single object or a list of objects):
[
  { "asks": [["4616.48", "91.6299"], ...], "bids": [["4616.47", "41.4772"], ...] },
  ...
]

Updates file (top-level list; each item either has `data.a`/`data.b` or directly `a`/`b`):
[
  { "data": { "e": "depthUpdate", "b": [["p","q"],...], "a": [["p","q"],...] } },
  { "b": [["p","q"],...], "a": [["p","q"],...] },
  ...
]

Semantics
---------
- For an update pair [price, qty]:
  - qty == "0" → remove that level from the book.
  - qty  > "0" → upsert that price level to the given quantity.
- Internally, the book is kept as dicts of strings for exactness: {side: {price_str: qty_str}}.
- When exporting/printing, sides are sorted: bids by descending price, asks by ascending price.

Usage
-----
# 1) Apply updates starting from a snapshot; store every step and dump to a file
python orderbook_local.py --snapshot snapshots.json --updates updates.json --store-every 1 --dump book_states.json

# 2) Only keep the final state and print top 10 levels to stdout
python orderbook_local.py --snapshot snapshots.json --updates updates.json --final-only --print-top 10

# 3) Start from an empty book (no snapshot) and apply updates
python orderbook_local.py --updates updates.json --final-only

# 4) Gzipped files are supported
python orderbook_local.py --snapshot snap.json.gz --updates upd.json.gz --dump states.json.gz

# 5) Generate a single revert update (apply it on the final book to get back to the initial snapshot)
python orderbook_local.py --snapshot snapshots.json --updates updates.json --emit-revert
# or save it to a file
python orderbook_local.py --snapshot snapshots.json --updates updates.json --save-revert revert.json

"""
from __future__ import annotations

import argparse
import gzip
import json
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple, Any, Iterable, Optional

# ----------------------------- IO helpers ---------------------------------

def open_text(path: str, mode: str = "rt"):
    if path.endswith(".gz"):
        return gzip.open(path, mode=mode, encoding="utf-8")
    return open(path, mode=mode, encoding="utf-8")


def dump_text(obj: Any, path: str) -> None:
    if path.endswith(".gz"):
        with gzip.open(path, mode="wt", encoding="utf-8") as fp:
            json.dump(obj, fp, separators=(",", ":"))
    else:
        with open(path, mode="wt", encoding="utf-8") as fp:
            json.dump(obj, fp, indent=2)

# ----------------------------- Parsing ------------------------------------

def parse_levels(levels: Any, label: str) -> List[Tuple[str, str]]:
    if levels is None:
        return []
    if not isinstance(levels, list):
        raise ValueError(f"'{label}' must be a list, got {type(levels).__name__}")
    out: List[Tuple[str, str]] = []
    for i, row in enumerate(levels):
        if not (isinstance(row, (list, tuple)) and len(row) == 2):
            raise ValueError(f"Each {label} level must be a [price, qty] pair (index {i})")
        p, q = row
        if not (isinstance(p, str) and isinstance(q, str)):
            raise ValueError(f"{label} values must be strings (index {i})")
        try:
            Decimal(p)
            qd = Decimal(q)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid decimal in {label} at index {i}: {row}") from exc
        if qd < 0:
            raise ValueError(f"Negative quantity in {label} at index {i}: {row}")
        out.append((p, q))
    return out


def iter_snapshots(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict) and ("asks" in obj or "bids" in obj):
        yield obj
        return
    if isinstance(obj, list):
        for i, item in enumerate(obj):
            if not isinstance(item, dict):
                raise ValueError(f"Snapshot at index {i} is not an object")
            yield item
        return
    raise ValueError("Snapshot JSON must be an object or a list of objects with 'asks'/'bids'.")


def iter_depth_events(obj: Any) -> Iterable[Dict[str, List[List[str]]]]:
    if not isinstance(obj, list):
        raise ValueError("Updates JSON must be a list of objects.")
    for i, item in enumerate(obj):
        if not isinstance(item, dict):
            raise ValueError(f"Update at index {i} is not an object")
        payload = item.get("data", item)
        if not isinstance(payload, dict):
            raise ValueError(f"Update payload at index {i} is not an object")
        a = payload.get("a", [])
        b = payload.get("b", [])
        if not isinstance(a, list) or not isinstance(b, list):
            raise ValueError(f"Update at index {i} missing 'a'/'b' arrays")
        yield {"a": a, "b": b}

# ----------------------------- Book ops -----------------------------------

def new_empty_book() -> Dict[str, Dict[str, str]]:
    return {"ask": {}, "bid": {}}


def book_from_snapshot(snap: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    asks = parse_levels(snap.get("asks", []), "asks")
    bids = parse_levels(snap.get("bids", []), "bids")
    book = new_empty_book()
    for p, q in asks:
        book["ask"][p] = q
    for p, q in bids:
        book["bid"][p] = q
    return book


def apply_event(book: Dict[str, Dict[str, str]], event: Dict[str, List[List[str]]]) -> None:
    # Bids
    for p, q in parse_levels(event.get("b"), "bids"):
        if Decimal(q) == 0:
            book["bid"].pop(p, None)
        else:
            book["bid"][p] = q
    # Asks
    for p, q in parse_levels(event.get("a"), "asks"):
        if Decimal(q) == 0:
            book["ask"].pop(p, None)
        else:
            book["ask"][p] = q


def generate_revert_update(current_book: Dict[str, Dict[str, str]], target_book: Dict[str, Dict[str, str]], zero_fmt: str = "0.00000000") -> Dict[str, Any]:
    """
    Build a single depthUpdate event that, when applied to `current_book`, yields `target_book`.

    Rules per price level:
      - If price exists in current but not in target -> set to zero (remove).
      - If price exists in target but not in current -> set to target qty.
      - If price exists in both with different qty -> set to target qty.
      - If price exists in both with same qty -> omit.
    """
    def side_diff(side: str) -> List[List[str]]:
        cur = current_book[side]
        tgt = target_book[side]
        out: List[List[str]] = []
        # Removals and changes
        for p, q_cur in cur.items():
            q_tgt = tgt.get(p)
            if q_tgt is None:
                out.append([p, zero_fmt])  # remove
            elif q_tgt != q_cur:
                out.append([p, q_tgt])     # change
        # Additions
        for p, q_tgt in tgt.items():
            if p not in cur:
                out.append([p, q_tgt])
        # Sort for readability (asks asc, bids desc)
        if side == "ask":
            out.sort(key=lambda pq: Decimal(pq[0]))
        else:
            out.sort(key=lambda pq: Decimal(pq[0]), reverse=True)
        return out

    return {
        "data": {
            "e": "depthUpdate",
            "b": side_diff("bid"),
            "a": side_diff("ask"),
        }
    }


def sorted_side(book_side: Dict[str, str], side: str) -> List[Tuple[str, str]]:
    if side == "ask":
        prices = sorted(book_side.keys(), key=lambda s: Decimal(s))
    else:
        prices = sorted(book_side.keys(), key=lambda s: Decimal(s), reverse=True)
    return [(p, book_side[p]) for p in prices]


def book_to_levels(book: Dict[str, Dict[str, str]]) -> Dict[str, List[List[str]]]:
    return {
        "asks": [[p, q] for p, q in sorted_side(book["ask"], "ask")],
        "bids": [[p, q] for p, q in sorted_side(book["bid"], "bid")],
    }

# ----------------------------- CLI ----------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Apply depth updates to an order book (in-memory)")
    ap.add_argument("--snapshot", default=None, help="Path to initial snapshot JSON (.json or .json.gz)")
    ap.add_argument("--updates", required=True, help="Path to updates JSON (.json or .json.gz)")
    ap.add_argument("--store-every", type=int, default=1, help="Store one state every N events (default: 1)")
    ap.add_argument("--final-only", action="store_true", help="Only keep the final state (overrides --store-every)")
    ap.add_argument("--print-top", type=int, default=0, help="Print top N levels of final book to stdout")
    ap.add_argument("--dump", default=None, help="Path to dump the collection of stored states as JSON (.json or .json.gz)")
    # Revert generation
    ap.add_argument("--emit-revert", action="store_true", help="Print a single depthUpdate that reverts the final book to the initial snapshot")
    ap.add_argument("--save-revert", default=None, help="Write the revert update JSON to this file (.json or .json.gz)")

    args = ap.parse_args(argv)

    # --- Build initial book ---
    if args.snapshot:
        with open_text(args.snapshot) as fp:
            snap_payload = json.load(fp)
        last_snap = None
        for last_snap in iter_snapshots(snap_payload):
            pass
        if last_snap is None:
            raise SystemExit("Snapshot file contains no usable objects.")
        initial_book = book_from_snapshot(last_snap)
    else:
        initial_book = new_empty_book()

    # Work copy that will be mutated by updates
    book = {"ask": dict(initial_book["ask"]), "bid": dict(initial_book["bid"]) }

    # --- Apply updates ---
    with open_text(args.updates) as fp:
        upd_payload = json.load(fp)

    states: List[Dict[str, List[List[str]]]] = []

    if args.final_only:
        for ev in iter_depth_events(upd_payload):
            apply_event(book, ev)
        states.append(book_to_levels(book))
    else:
        n = 0
        for ev in iter_depth_events(upd_payload):
            n += 1
            apply_event(book, ev)
            if n % max(1, args.store_every) == 0:
                states.append(book_to_levels(book))

    # --- Optional stdout summary (final state) ---
    final = book_to_levels(book)
    if args.print_top > 0:
        N = args.print_top
        print("Final top bids:")
        for p, q in final["bids"][:N]:
            print(f"  {p}\t{q}")
        print("Final top asks:")
        for p, q in final["asks"][:N]:
            print(f"  {p}\t{q}")

    # --- Optional dump of stored states ---
    if args.dump:
        dump_text(states, args.dump)

    # --- Revert update generation (final -> initial) ---
    if args.emit_revert or args.save_revert:
        revert = generate_revert_update(
            current_book={"ask": dict(book["ask"]), "bid": dict(book["bid"])},
            target_book={"ask": dict(initial_book["ask"]), "bid": dict(initial_book["bid"])},
        )
        if args.emit_revert:
            print(json.dumps(revert, indent=2))
        if args.save_revert:
            dump_text(revert, args.save_revert)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
