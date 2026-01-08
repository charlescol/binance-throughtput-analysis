import sys
import argparse
from pathlib import Path
import yaml


def load_symbols(yaml_path: Path) -> set[str]:
    """Load the 'symbols' list from a YAML file and return it as a set."""
    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    symbols = data.get("symbols", [])
    if symbols is None:
        symbols = []

    if not isinstance(symbols, list):
        raise ValueError(f"'symbols' must be a list in {yaml_path}")

    # Normalize: ensure strings, strip whitespace, drop empties
    out: set[str] = set()
    for s in symbols:
        if s is None:
            continue
        if not isinstance(s, str):
            s = str(s)
        s = s.strip()
        if s:
            out.add(s)

    return out


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute common or uncommon symbols between two YAML files."
    )
    parser.add_argument("file1", type=Path, help="First YAML file")
    parser.add_argument("file2", type=Path, help="Second YAML file")
    parser.add_argument(
        "-u",
        "--uncommon",
        action="store_true",
        help="Show uncommon symbols (symmetric difference) instead of common symbols",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    file1: Path = args.file1
    file2: Path = args.file2

    if not file1.exists():
        print(f"Error: file not found: {file1}")
        return 2
    if not file2.exists():
        print(f"Error: file not found: {file2}")
        return 2

    symbols1 = load_symbols(file1)
    symbols2 = load_symbols(file2)

    common = symbols1 & symbols2
    uncommon = symbols1 ^ symbols2  # symmetric difference

    print(f"Symbols in {file1.name}: {len(symbols1)}")
    print(f"Symbols in {file2.name}: {len(symbols2)}")

    if args.uncommon:
        target = uncommon
        label = "Uncommon symbols"
    else:
        target = common
        label = "Common symbols"

    print(f"{label}: {len(target)}")

    if target:
        print(f"\nList of {label.lower()}:")
        for s in sorted(target):
            print(f"  - {s}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))