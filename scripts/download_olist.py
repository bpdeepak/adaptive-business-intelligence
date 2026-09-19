"""Download the Olist Brazilian E-Commerce public dataset (Phase 0).

Pulls all 9 CSVs from a verified public mirror of the official Kaggle
dataset into ``data/raw/`` and verifies each file's expected byte size.
Stdlib only — no third-party dependencies required.

Usage:
    uv run python scripts/download_olist.py [--force]
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import urllib.request

MIRROR_BASE = (
    "https://raw.githubusercontent.com/ductransponster/"
    "olist-brazilian-ecommerce/main/data/original_data"
)

# (filename, expected_size_bytes) — sizes taken from the official Kaggle release.
FILES: list[tuple[str, int]] = [
    ("olist_customers_dataset.csv", 9_033_957),
    ("olist_geolocation_dataset.csv", 61_273_883),
    ("olist_order_items_dataset.csv", 15_438_671),
    ("olist_order_payments_dataset.csv", 5_777_138),
    ("olist_order_reviews_dataset.csv", 14_346_950),
    ("olist_orders_dataset.csv", 17_654_914),
    ("olist_products_dataset.csv", 2_379_446),
    ("olist_sellers_dataset.csv", 174_703),
    ("product_category_name_translation.csv", 2_542),
]

UA = "abi-bootstrap/0.1 (adaptive-business-intelligence)"


def download(url: str, dest: pathlib.Path, expected: int, retries: int = 3) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as resp:
                with open(tmp, "wb") as out:
                    while chunk := resp.read(1 << 20):
                        out.write(chunk)
            actual = tmp.stat().st_size
            if actual != expected:
                raise RuntimeError(f"size mismatch: got {actual:,}, want {expected:,}")
            tmp.replace(dest)
            return
        except Exception as exc:  # noqa: BLE001
            print(f"  attempt {attempt} failed for {dest.name}: {exc}", file=sys.stderr)
            tmp.unlink(missing_ok=True)
    raise RuntimeError(f"failed to download {dest.name} after {retries} attempts")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="re-download files even when cached size matches")
    args = parser.parse_args()

    raw = pathlib.Path(__file__).resolve().parent.parent / "data" / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    for name, size in FILES:
        dest = raw / name
        if dest.exists() and not args.force and dest.stat().st_size == size:
            print(f"  cached {name} ({size:,} bytes)")
            continue
        print(f"  down   {name} ({size:,} bytes)")
        download(f"{MIRROR_BASE}/{name}", dest, size)

    total = sum(p.stat().st_size for p in raw.iterdir() if p.is_file())
    print(f"\nAll {len(FILES)} files present under {raw} ({total:,} bytes total).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())