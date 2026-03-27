"""
ASTRO Secondary Screen — 不動産情報ライブラリ API Data Fetcher
=============================================================
Fetches spatial data for 152 qualified municipalities from reinfolib APIs,
caches as GeoJSON files for the secondary screen.

Usage:
    python fetch_reinfolib.py --apikey YOUR_KEY --layer xkt003
    python fetch_reinfolib.py --apikey YOUR_KEY --all
    python fetch_reinfolib.py --apikey YOUR_KEY --zone zone_23ku --layer xkt003

Layers:
    xkt003  立地適正化計画区域 (★最優先)
    xkt004  用途地域
    xkt005  防火・準防火地域
    xkt006  学校
    xkt007  保育園・幼稚園
    xkt008  医療機関
    xkt009  洪水浸水想定区域
    xkt010  土砂災害警戒区域
    xkt013  将来推計人口 (250mメッシュ)
    xkt015  駅別乗降客数

API仕様:
    Base URL: https://www.reinfolib.mlit.go.jp/ex-api/external/{API_CODE}
    認証: Header 'Ocp-Apim-Subscription-Key'
    座標系: XYZタイル (z/x/y)
    出力: GeoJSON
"""

import argparse
import gzip
import json
import math
import os
import sys
import time
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
BASE_URL = "https://www.reinfolib.mlit.go.jp/ex-api/external"

# Layer definitions
LAYERS = {
    "xkt003": {"name": "立地適正化計画区域", "priority": 1, "zoom": 14},
    "xkt004": {"name": "用途地域", "priority": 4, "zoom": 14},
    "xkt005": {"name": "防火・準防火地域", "priority": 4, "zoom": 14},
    "xkt006": {"name": "学校", "priority": 3, "zoom": 14},
    "xkt007": {"name": "保育園・幼稚園", "priority": 3, "zoom": 14},
    "xkt008": {"name": "医療機関", "priority": 3, "zoom": 14},
    "xkt009": {"name": "洪水浸水想定区域", "priority": 5, "zoom": 14},
    "xkt010": {"name": "土砂災害警戒区域", "priority": 5, "zoom": 14},
    "xkt013": {"name": "将来推計人口", "priority": 2, "zoom": 14},
    "xkt015": {"name": "駅別乗降客数", "priority": 6, "zoom": 14},
}


# ============================================================
# Tile math
# ============================================================

def lat_lon_to_tile(lat, lon, zoom):
    """Convert lat/lon to XYZ tile coordinates."""
    n = 2 ** zoom
    x = int((lon + 180) / 360 * n)
    lat_rad = math.radians(lat)
    y = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
    return x, y


def get_tiles_for_bounds(min_lat, max_lat, min_lon, max_lon, zoom):
    """Get all tiles covering a bounding box."""
    x_min, y_max = lat_lon_to_tile(min_lat, min_lon, zoom)
    x_max, y_min = lat_lon_to_tile(max_lat, max_lon, zoom)
    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tiles.append((x, y))
    return tiles


def get_tiles_for_municipality(muni, zoom=14, buffer_km=1.0):
    """Get tiles covering a municipality with buffer."""
    lat, lon = muni["lat"], muni["lon"]
    # Estimate municipality extent (rough)
    buf_lat = buffer_km / 111  # ~1km in lat degrees
    buf_lon = buffer_km / (111 * math.cos(math.radians(lat)))
    
    return get_tiles_for_bounds(
        lat - buf_lat, lat + buf_lat,
        lon - buf_lon, lon + buf_lon,
        zoom
    )


# ============================================================
# API client
# ============================================================

def fetch_tile(apikey, layer_code, z, x, y, retries=3):
    """Fetch a single tile as GeoJSON."""
    url = f"{BASE_URL}/{layer_code}?response_format=geojson&z={z}&x={x}&y={y}"
    req = urllib.request.Request(url)
    req.add_header("Ocp-Apim-Subscription-Key", apikey)
    req.add_header("Accept-Encoding", "gzip")

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read()
                encoding = (resp.headers.get("Content-Encoding") or "").lower()
                if "gzip" in encoding:
                    data = gzip.decompress(data)
                return json.loads(data)
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Rate limited
                wait = (attempt + 1) * 5
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif e.code == 404:
                return None  # No data for this tile
            else:
                print(f"    HTTP {e.code} for {z}/{x}/{y}")
                return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"    Error: {e}")
                return None

    return None


# ============================================================
# Fetch and merge
# ============================================================

def fetch_layer_for_zone(apikey, layer_code, zone_data, cache_dir, delay=1.0):
    """
    Fetch all tiles for a zone's municipalities, merge into a single GeoJSON.
    Caches individual tiles and skips already-fetched ones.
    """
    zoom = LAYERS[layer_code]["zoom"]
    layer_cache = cache_dir / layer_code
    layer_cache.mkdir(parents=True, exist_ok=True)

    # Collect all unique tiles needed
    all_tiles = set()
    for muni in zone_data["municipalities"]:
        if not muni.get("lat") or not muni.get("lon"):
            continue
        # Use zone bounds instead of per-municipality for efficiency
        bounds = zone_data.get("bounds", {})
        if bounds:
            tiles = get_tiles_for_bounds(
                bounds["minLat"], bounds["maxLat"],
                bounds["minLon"], bounds["maxLon"],
                zoom
            )
            all_tiles.update(tiles)
            break  # Only need to do this once per zone

    if not all_tiles:
        # Fallback: compute per municipality
        for muni in zone_data["municipalities"]:
            if muni.get("lat") and muni.get("lon"):
                tiles = get_tiles_for_municipality(muni, zoom, buffer_km=2.0)
                all_tiles.update(tiles)

    print(f"  Tiles to fetch: {len(all_tiles)}")

    # Fetch tiles (with cache)
    all_features = []
    fetched = 0
    cached = 0
    errors = 0

    for x, y in sorted(all_tiles):
        tile_file = layer_cache / f"{zoom}_{x}_{y}.json"

        if tile_file.exists():
            with open(tile_file) as f:
                tile_data = json.load(f)
            cached += 1
        else:
            tile_data = fetch_tile(apikey, layer_code.upper(), zoom, x, y)
            if tile_data:
                with open(tile_file, "w") as f:
                    json.dump(tile_data, f)
                fetched += 1
                time.sleep(delay)  # Rate limit
            else:
                errors += 1
                continue

        # Merge features
        if tile_data and tile_data.get("type") == "FeatureCollection":
            all_features.extend(tile_data.get("features", []))

    print(f"  Fetched: {fetched}, Cached: {cached}, Errors: {errors}")
    print(f"  Total features: {len(all_features)}")

    # Save merged GeoJSON
    merged = {
        "type": "FeatureCollection",
        "features": all_features
    }

    return merged


def fetch_layer_for_all_zones(apikey, layer_code, zones, cache_dir, delay=1.0):
    """Fetch a layer for all zones."""
    layer_name = LAYERS[layer_code]["name"]
    print(f"\n{'='*60}")
    print(f"Fetching: {layer_code} ({layer_name})")
    print(f"{'='*60}")

    results = {}
    for zone_id, zone_data in zones.items():
        print(f"\n  [{zone_id}] {zone_data['name']} ({zone_data['count']} municipalities)")
        merged = fetch_layer_for_zone(apikey, layer_code, zone_data, cache_dir, delay)

        # Save zone-level merged file
        out_file = cache_dir / layer_code / f"{zone_id}.json"
        with open(out_file, "w") as f:
            json.dump(merged, f, ensure_ascii=False)
        print(f"  Saved: {out_file} ({len(merged['features'])} features)")

        results[zone_id] = {
            "features": len(merged["features"]),
            "file": str(out_file),
        }

    return results


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="ASTRO Secondary Screen — reinfolib API data fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--apikey", required=True, help="不動産情報ライブラリ APIキー")
    parser.add_argument("--layer", help="Fetch specific layer (e.g., xkt003)")
    parser.add_argument("--all", action="store_true", help="Fetch all layers")
    parser.add_argument("--zone", help="Fetch for specific zone only (e.g., zone_23ku)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--list", action="store_true", help="List available layers")

    args = parser.parse_args()

    if args.list:
        print("Available layers:")
        for code, info in sorted(LAYERS.items(), key=lambda x: x[1]["priority"]):
            print(f"  {code}  P{info['priority']}  {info['name']}")
        return

    # Load zones
    zones_path = SCRIPT_DIR / "zones.json"
    if not zones_path.exists():
        print("ERROR: zones.json not found. Run build_dashboard.py first.")
        sys.exit(1)

    with open(zones_path) as f:
        zones = json.load(f)

    # Filter zone if specified
    if args.zone:
        if args.zone not in zones:
            print(f"ERROR: Unknown zone '{args.zone}'. Available: {list(zones.keys())}")
            sys.exit(1)
        zones = {args.zone: zones[args.zone]}

    cache_dir = SCRIPT_DIR / "cache"
    cache_dir.mkdir(exist_ok=True)

    if args.all:
        for layer_code in sorted(LAYERS, key=lambda x: LAYERS[x]["priority"]):
            fetch_layer_for_all_zones(args.apikey, layer_code, zones, cache_dir, args.delay)
    elif args.layer:
        layer_code = args.layer.lower()
        if layer_code not in LAYERS:
            print(f"ERROR: Unknown layer '{layer_code}'. Use --list to see available layers.")
            sys.exit(1)
        fetch_layer_for_all_zones(args.apikey, layer_code, zones, cache_dir, args.delay)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
