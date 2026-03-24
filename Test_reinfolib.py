"""
不動産情報ライブラリ API — 接続テスト
====================================
立地適正化計画（XKT003）を含む主要APIの接続テスト。

Usage:
    python test_reinfolib.py --apikey YOUR_REINFOLIB_API_KEY

APIキーの取得:
    https://www.reinfolib.mlit.go.jp/api/request/
    申請後5営業日以内に発行される。

API仕様:
    - XYZタイル座標で指定 (z/x/y)
    - GeoJSON or PBF形式
    - ヘッダーに Ocp-Apim-Subscription-Key を設定
"""

import argparse
import gzip
import json
import math
import sys
import urllib.request

BASE_URL = "https://www.reinfolib.mlit.go.jp/ex-api/external"

# 二次スクリーンで使うAPI一覧
APIS = {
    # 都市計画系
    "XKT001": "都市計画区域",
    "XKT002": "区域区分",
    "XKT003": "立地適正化計画区域",          # ★最優先
    "XKT004": "用途地域",
    "XKT005": "防火・準防火地域",
    # 防災系
    "XKT009": "洪水浸水想定区域",
    "XKT010": "土砂災害警戒区域",
    # 人口系
    "XKT013": "将来推計人口（250mメッシュ）",  # ★年齢構成あり
    # 施設系
    "XKT006": "学校",
    "XKT015": "駅別乗降客数",
}

# テスト用タイル座標 (z=14 でおおむね1km²くらい)
# 川崎市中原区（ASTROスコア #3）あたり
TEST_TILES = {
    "川崎中原区": {"z": 14, "x": 14549, "y": 6467},
    "文京区":     {"z": 14, "x": 14553, "y": 6461},
    "流山市":     {"z": 14, "x": 14563, "y": 6448},
}


def lat_lon_to_tile(lat, lon, zoom):
    """緯度経度 → XYZタイル座標"""
    n = 2 ** zoom
    x = int((lon + 180) / 360 * n)
    lat_rad = math.radians(lat)
    y = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
    return x, y


def fetch_geojson(apikey, api_code, z, x, y, extra_params=None):
    """GeoJSON形式でAPIを叩く"""
    params = f"response_format=geojson&z={z}&x={x}&y={y}"
    if extra_params:
        params += "&" + "&".join(f"{k}={v}" for k, v in extra_params.items())
    
    url = f"{BASE_URL}/{api_code}?{params}"
    req = urllib.request.Request(url)
    req.add_header("Ocp-Apim-Subscription-Key", apikey)
    req.add_header("Accept-Encoding", "gzip")
    
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
            encoding = (resp.headers.get("Content-Encoding") or "").lower()
            if "gzip" in encoding:
                data = gzip.decompress(data)
            return json.loads(data)
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "message": e.read().decode()[:200]}
    except Exception as e:
        return {"error": str(e)}


def test_api(apikey, api_code, api_name, tile_name, tile):
    """単一APIテスト"""
    print(f"\n  [{api_code}] {api_name} @ {tile_name}")
    result = fetch_geojson(apikey, api_code, tile["z"], tile["x"], tile["y"])
    
    if "error" in result:
        print(f"    ✗ Error: {result['error']}")
        if "message" in result:
            print(f"      {result['message'][:100]}")
        return False
    
    # GeoJSON FeatureCollection
    if result.get("type") == "FeatureCollection":
        features = result.get("features", [])
        print(f"    ✓ {len(features)} features")
        if features:
            props = features[0].get("properties", {})
            geom_type = features[0].get("geometry", {}).get("type", "?")
            print(f"    Geometry: {geom_type}")
            print(f"    Properties ({len(props)} fields):")
            for k, v in list(props.items())[:8]:
                print(f"      {k}: {v}")
            if len(props) > 8:
                print(f"      ... and {len(props)-8} more fields")
        return True
    
    # Non-GeoJSON response (JSON list etc)
    if isinstance(result, dict) and "data" in result:
        data = result["data"]
        print(f"    ✓ {len(data)} records (JSON)")
        if data:
            print(f"    Fields: {list(data[0].keys())[:10]}")
        return True
    
    print(f"    ? Unexpected format: {str(result)[:200]}")
    return False


def main():
    parser = argparse.ArgumentParser(description="不動産情報ライブラリAPI接続テスト")
    parser.add_argument("--apikey", required=True, help="不動産情報ライブラリのAPIキー")
    parser.add_argument("--api", help="特定のAPIだけテスト (例: XKT003)")
    parser.add_argument("--tile", help="テスト地点 (例: 川崎中原区)")
    parser.add_argument("--lat", type=float, help="緯度（--lonも必須）")
    parser.add_argument("--lon", type=float, help="経度（--latも必須）")
    args = parser.parse_args()
    
    print("=" * 60)
    print("不動産情報ライブラリ API 接続テスト")
    print("=" * 60)
    
    # Determine test tiles
    if args.lat and args.lon:
        x, y = lat_lon_to_tile(args.lat, args.lon, 14)
        tiles = {"custom": {"z": 14, "x": x, "y": y}}
        print(f"Custom location: ({args.lat}, {args.lon}) → tile(14/{x}/{y})")
    elif args.tile and args.tile in TEST_TILES:
        tiles = {args.tile: TEST_TILES[args.tile]}
    else:
        tiles = {"川崎中原区": TEST_TILES["川崎中原区"]}  # default
    
    # Determine APIs to test
    if args.api:
        apis = {args.api: APIS.get(args.api, args.api)}
    else:
        apis = APIS
    
    # Run tests
    success = 0
    total = 0
    for tile_name, tile in tiles.items():
        print(f"\n{'─'*40}")
        print(f"Test location: {tile_name} (z={tile['z']}, x={tile['x']}, y={tile['y']})")
        print(f"{'─'*40}")
        
        for api_code, api_name in apis.items():
            total += 1
            if test_api(args.apikey, api_code, api_name, tile_name, tile):
                success += 1
    
    print(f"\n{'='*60}")
    print(f"Results: {success}/{total} APIs responded successfully")
    if success < total:
        print(f"  {total-success} failed — check API key or API availability")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()