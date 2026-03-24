"""
ASTRO Heatmap 2026 — e-Stat API Data Fetcher
=============================================
Fetches 4 macro indicators from e-Stat API and outputs CSVs
compatible with the heatmap_GTA/data/ directory structure.

Usage:
    python fetch_estat.py --appid YOUR_ESTAT_APPID

To get an appId (free):
    1. Go to https://www.e-stat.go.jp/api/
    2. Click ユーザ登録 → register
    3. Go to マイページ → アプリケーションID → 発行

Outputs (written to data/ directory):
    raw_population_10y.csv   — Total population by municipality, 10Y CAGR
    raw_under20_2039.csv     — Under-20 and 20-39 population, 10Y CAGR
    raw_taxable_income.csv   — Taxable income by municipality, 7Y CAGR
    scores_macro_detail.csv  — Scored indicators (1-10 scale) + Macro total
    scores_total.csv         — Updated Total = Macro + Station
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

# ============================================================
# Config
# ============================================================

ESTAT_API_BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"

# Target prefectures (Kanto 1都4県 used in ASTRO)
TARGET_PREFS = {"08": "茨城県", "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県"}

# Years to fetch for population (住民基本台帳)
POP_YEARS = list(range(2015, 2026))  # 2015-2025

# Years for taxable income (市町村税課税状況等の調)
INCOME_YEARS = list(range(2017, 2025))  # 2017-2024

# Scoring thresholds (from scoring_thresholds.csv — percentile breakpoints)
# Format: [q1, q2, ..., q10, q11] boundaries for scores 1-10
THRESHOLDS = {
    "population": [-0.0714, -0.0171, -0.013435, -0.007223, -0.004017,
                   -0.001707, 0.000714, 0.002663, 0.004534, 0.006832, 0.030507],
    "under20":    [-0.072495, -0.038382, -0.03099, -0.025443, -0.020393,
                   -0.016199, -0.013462, -0.009486, -0.005583, 0.002955, 0.044285],
    "age2039":    [-0.064206, -0.036192, -0.027127, -0.021737, -0.016008,
                   -0.010901, -0.008229, -0.005191, -0.000487, 0.005208, 0.025557],
    "income":     [-0.0672, 0.001797, 0.007995, 0.011961, 0.015165,
                    0.019306, 0.023589, 0.025901, 0.030076, 0.033488, 0.101601],
}

# Macro coefficients (which indicators are included and their weight)
# Verified against Excel: Macro = pop + under20 + age2039 + income + safety
# land_price, land_price_cagr, park_space are tracked but NOT in the total
MACRO_COEFFS = {
    "population": 1,
    "under20": 1,
    "age2039": 1,
    "income": 1,
    # safety: 1 (not fetched via API — loaded from existing CSV)
    # land_price: 0 (tracked but excluded from total)
    # land_price_cagr: 0 (tracked but excluded from total)
    # park_space: 0 (tracked but excluded from total)
}


# ============================================================
# e-Stat API helpers
# ============================================================

def estat_request(appid: str, endpoint: str, params: dict) -> dict:
    """Make a request to e-Stat API."""
    params["appId"] = appid
    url = f"{ESTAT_API_BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "ASTRO-Heatmap/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  [ERROR] API request failed: {e}")
        print(f"  URL: {url[:200]}...")
        return None


def search_stats(appid: str, keyword: str, limit: int = 10) -> list:
    """Search for statistical tables by keyword."""
    data = estat_request(appid, "getStatsList", {
        "searchWord": keyword,
        "limit": str(limit),
        "lang": "J",
    })
    if not data:
        return []
    result = data.get("GET_STATS_LIST", {}).get("DATALIST_INF", {}).get("TABLE_INF", [])
    if isinstance(result, dict):
        result = [result]
    return result


def get_stats_data(appid: str, stats_data_id: str, **kwargs) -> dict:
    """Fetch data from a specific statistical table."""
    params = {"statsDataId": stats_data_id, "lang": "J"}
    params.update(kwargs)
    data = estat_request(appid, "getStatsData", params)
    if not data:
        return None
    return data.get("GET_STATS_DATA", {})


def parse_value_data(stats_data: dict) -> list:
    """Parse the VALUE array from e-Stat response into a list of dicts."""
    if not stats_data:
        return []
    data_inf = stats_data.get("STATISTICAL_DATA", {}).get("DATA_INF", {})
    values = data_inf.get("VALUE", [])
    if isinstance(values, dict):
        values = [values]

    # Also get class info for decoding category codes
    class_inf = stats_data.get("STATISTICAL_DATA", {}).get("CLASS_INF", {}).get("CLASS_OBJ", [])
    if isinstance(class_inf, dict):
        class_inf = [class_inf]

    class_maps = {}
    for cls in class_inf:
        cls_id = cls.get("@id", "")
        items = cls.get("CLASS", [])
        if isinstance(items, dict):
            items = [items]
        class_maps[cls_id] = {item.get("@code", ""): item.get("@name", "") for item in items}

    return values, class_maps


# ============================================================
# Data fetchers for each indicator
# ============================================================

def fetch_population_by_age(appid: str) -> dict:
    """
    Fetch 住民基本台帳人口 (年齢階級別) from e-Stat.
    Returns: {code: {year: {age_group: population}}}

    Table: 住民基本台帳に基づく人口、人口動態及び世帯数
    The exact statsDataId changes each year, so we search for it.
    """
    print("\n[1/2] Fetching population data (住民基本台帳)...")

    # Search for the table
    tables = search_stats(appid, "住民基本台帳 年齢 市区町村", limit=30)
    if not tables:
        print("  Could not find population tables. Trying alternative search...")
        tables = search_stats(appid, "住民基本台帳人口 年齢階級別", limit=30)

    print(f"  Found {len(tables)} candidate tables:")
    for t in tables[:10]:
        tid = t.get("@id", "")
        title = t.get("TITLE", {})
        if isinstance(title, dict):
            title = title.get("$", "")
        survey = t.get("STATISTICS_NAME", "")
        print(f"    {tid}: {title[:60]} ({survey})")

    # The user will need to identify the correct table ID
    # For now, we'll try the standard approach
    print("\n  [INFO] Population data requires identifying the correct statsDataId.")
    print("  Please check the table list above and update POPULATION_TABLE_ID in config.")
    print("  Common pattern: '0003448XXXX' for 住民基本台帳年齢階級別人口")

    return {}


def fetch_taxable_income(appid: str) -> dict:
    """
    Fetch 課税対象所得 from e-Stat.
    Table: 市町村税課税状況等の調
    """
    print("\n[2/2] Fetching taxable income data (課税対象所得)...")

    tables = search_stats(appid, "市町村税課税状況等 課税対象所得", limit=20)
    if not tables:
        tables = search_stats(appid, "市町村税 課税所得", limit=20)

    print(f"  Found {len(tables)} candidate tables:")
    for t in tables[:10]:
        tid = t.get("@id", "")
        title = t.get("TITLE", {})
        if isinstance(title, dict):
            title = title.get("$", "")
        print(f"    {tid}: {title[:80]}")

    return {}


# ============================================================
# Scoring functions
# ============================================================

def cagr(start: float, end: float, years: int) -> float:
    """Compound Annual Growth Rate."""
    if start <= 0 or end <= 0 or years <= 0:
        return None
    return (end / start) ** (1 / years) - 1


def score_value(value: float, thresholds: list) -> int:
    """Score a value 1-10 based on percentile thresholds."""
    if value is None:
        return None
    for i in range(len(thresholds) - 1):
        if value <= thresholds[i + 1]:
            return i + 1
    return 10


def compute_macro_scores(pop_cagr, under20_cagr, age2039_cagr, income_cagr, safety_score):
    """Compute individual scores and Macro total."""
    scores = {
        "pop": score_value(pop_cagr, THRESHOLDS["population"]),
        "under20": score_value(under20_cagr, THRESHOLDS["under20"]),
        "age2039": score_value(age2039_cagr, THRESHOLDS["age2039"]),
        "income": score_value(income_cagr, THRESHOLDS["income"]),
        "safety": safety_score,  # pass-through from existing data
    }

    # Macro total = sum of (score * coeff)
    macro = 0
    for key, coeff in MACRO_COEFFS.items():
        s = scores.get(key)
        if s is not None:
            macro += s * coeff
    if safety_score is not None:
        macro += safety_score  # safety coeff = 1

    scores["macro_total"] = macro
    return scores


# ============================================================
# Code format helpers
# ============================================================

def to_dashboard_code(raw_code: str) -> str:
    """
    Convert a municipal code to 5-digit dashboard format (no check digit).
    Handles both formats:
      - Check digit format: '82015' -> '08201', '111007' -> '11100'
      - Already dashboard format: '08201' -> '08201', '11100' -> '11100'
    """
    raw_code = str(raw_code).strip()
    if not raw_code or not raw_code.replace('.0', '').isdigit():
        return raw_code
    c = int(float(raw_code))
    if raw_code.startswith('0'):
        # Already has leading zero = dashboard format
        return raw_code.zfill(5)
    if c > 14999:
        # Check-digit format (5-6 digits, value > max prefecture prefix 14xxx)
        return str(c // 10).zfill(5)
    else:
        # Already dashboard format stored as int
        return str(c).zfill(5)


def load_existing_csv(filepath: str) -> list:
    """Load an existing CSV as list of dicts."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(filepath: str, rows: list, fieldnames: list):
    """Write a list of dicts to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written: {filepath} ({len(rows)} rows)")


# ============================================================
# Main pipeline
# ============================================================

def discover_tables(appid: str):
    """Step 1: Discover available e-Stat tables for our indicators."""
    print("=" * 70)
    print("ASTRO Heatmap — e-Stat Table Discovery")
    print("=" * 70)

    fetch_population_by_age(appid)
    fetch_taxable_income(appid)

    print("\n" + "=" * 70)
    print("NEXT STEPS:")
    print("=" * 70)
    print("""
1. From the table lists above, identify the correct statsDataId values.
2. Update the config section in this script:
   - POPULATION_TABLE_IDS: dict mapping year -> statsDataId
   - INCOME_TABLE_IDS: dict mapping year -> statsDataId
3. Run again with: python fetch_estat.py --appid YOUR_ID --fetch
    """)


def fetch_and_export(appid: str, data_dir: str):
    """Step 2: Fetch data from identified tables and compute scores."""
    print("=" * 70)
    print("ASTRO Heatmap — Fetch & Score Pipeline")
    print("=" * 70)

    # Load existing data for indicators we can't auto-fetch
    existing_safety = {}
    safety_path = os.path.join(data_dir, "raw_crime.csv")
    if os.path.exists(safety_path):
        for row in load_existing_csv(safety_path):
            code = row.get("code", "")
            # Use most recent year's crime count
            crimes = row.get("crimes_2025") or row.get("crimes_2024") or row.get("crimes_2023")
            if crimes:
                existing_safety[code] = crimes
        print(f"  Loaded {len(existing_safety)} safety records from existing data")

    existing_station = {}
    station_path = os.path.join(data_dir, "raw_station.csv")
    if os.path.exists(station_path):
        for row in load_existing_csv(station_path):
            code_raw = row.get("code", "")
            score = row.get("score", "")
            if score and code_raw:
                existing_station[to_dashboard_code(code_raw)] = float(score)
        print(f"  Loaded {len(existing_station)} station scores from existing data")

    print("\n  [INFO] Full fetch requires configured table IDs.")
    print("  Run with --discover first to find the correct table IDs.")
    print("  For now, demonstrating the scoring pipeline with existing data...")

    # Demo: recompute scores from existing raw CSVs
    recompute_from_existing(data_dir, existing_safety, existing_station)


def recompute_from_existing(data_dir: str, safety_data: dict, station_data: dict):
    """Recompute all scores from existing raw CSVs (useful for recalibration)."""
    print("\n  Recomputing scores from existing raw data...")

    # Load station data if not provided
    if not station_data:
        station_path = os.path.join(data_dir, "raw_station.csv")
        if os.path.exists(station_path):
            for row in load_existing_csv(station_path):
                code_raw = row.get("code", "")
                score = row.get("score", "")
                if score and code_raw:
                    station_data[to_dashboard_code(code_raw)] = float(score)
            print(f"  Loaded {len(station_data)} station scores")

    # Load population data (codes: with check digit -> convert)
    pop_data = {}
    pop_path = os.path.join(data_dir, "raw_population_10y.csv")
    if os.path.exists(pop_path):
        for row in load_existing_csv(pop_path):
            code = to_dashboard_code(row["code"])
            cagr_val = row.get("cagr_10y", "")
            pop_data[code] = float(cagr_val) if cagr_val else None

    # Load under20 / 20-39 data
    age_data = {}
    age_path = os.path.join(data_dir, "raw_under20_2039.csv")
    if os.path.exists(age_path):
        for row in load_existing_csv(age_path):
            code = to_dashboard_code(row["code"])
            u20 = row.get("under20_cagr_10y", "")
            a2039 = row.get("age2039_cagr_10y", "")
            age_data[code] = {
                "under20": float(u20) if u20 else None,
                "age2039": float(a2039) if a2039 else None,
            }

    # Load income data
    income_data = {}
    income_path = os.path.join(data_dir, "raw_taxable_income.csv")
    if os.path.exists(income_path):
        for row in load_existing_csv(income_path):
            code = to_dashboard_code(row["code"])
            cagr_val = row.get("cagr_7y", "")
            income_data[code] = float(cagr_val) if cagr_val else None

    # Load existing macro detail for safety scores + park/land price
    # (these use codes with check digit from original export)
    macro_detail = {}
    macro_path = os.path.join(data_dir, "scores_macro_detail.csv")
    if os.path.exists(macro_path):
        for row in load_existing_csv(macro_path):
            code = to_dashboard_code(row["code"])
            macro_detail[code] = row

    # Get all codes
    all_codes = sorted(set(pop_data.keys()) | set(age_data.keys()) | set(income_data.keys()) | set(macro_detail.keys()))
    print(f"  Processing {len(all_codes)} municipalities...")

    # Compute scores
    results = []
    for code in all_codes:
        pop_cagr = pop_data.get(code)
        ages = age_data.get(code, {})
        inc_cagr = income_data.get(code)

        # Safety score from existing detail
        detail = macro_detail.get(code, {})
        safety_score_str = detail.get("score_safety", "")
        safety_score = int(safety_score_str) if safety_score_str else None

        # Park score from existing detail
        park_score_str = detail.get("score_park", "")
        park_score = int(park_score_str) if park_score_str else None

        # Land price scores from existing
        lp_score_str = detail.get("score_land_price", "")
        lp_score = int(lp_score_str) if lp_score_str else None
        lp_cagr_score_str = detail.get("score_land_cagr", "")
        lp_cagr_score = int(lp_cagr_score_str) if lp_cagr_score_str else None

        # Compute API-based scores
        s_pop = score_value(pop_cagr, THRESHOLDS["population"])
        s_u20 = score_value(ages.get("under20"), THRESHOLDS["under20"])
        s_2039 = score_value(ages.get("age2039"), THRESHOLDS["age2039"])
        s_inc = score_value(inc_cagr, THRESHOLDS["income"])

        # Macro total = pop + under20 + age2039 + income + safety (5 indicators, max 50)
        macro = 0
        for s in [s_pop, s_u20, s_2039, s_inc, safety_score]:
            if s is not None:
                macro += s

        # Station score (raw × 5 = Train Coeff from Excel)
        stn_raw = station_data.get(code)
        stn_score = round(stn_raw * 5, 1) if stn_raw is not None else None

        # Total = Macro + Station (already multiplied), or Macro × 1.5 if no station
        if stn_score is not None:
            total = macro + stn_score
        else:
            total = macro * 1.5

        results.append({
            "code": to_dashboard_code(code),
            "prefecture": detail.get("prefecture", ""),
            "city_en": detail.get("city_en", ""),
            "city_jp": detail.get("city_jp", ""),
            "pop_cagr": f"{pop_cagr:.6f}" if pop_cagr is not None else "",
            "under20_cagr": f"{ages.get('under20', ''):.6f}" if ages.get("under20") is not None else "",
            "age2039_cagr": f"{ages.get('age2039', ''):.6f}" if ages.get("age2039") is not None else "",
            "income_cagr": f"{inc_cagr:.6f}" if inc_cagr is not None else "",
            "score_pop": s_pop if s_pop else "",
            "score_under20": s_u20 if s_u20 else "",
            "score_2039": s_2039 if s_2039 else "",
            "score_income": s_inc if s_inc else "",
            "score_safety": safety_score if safety_score else "",
            "score_land_price": lp_score if lp_score else "",
            "score_land_cagr": lp_cagr_score if lp_cagr_score else "",
            "score_park": park_score if park_score else "",
            "macro_total": macro,
            "station_score": round(stn_score, 1) if stn_score else "",
            "total_score": round(total, 1),
        })

    # Write scores
    write_csv(
        os.path.join(data_dir, "scores_macro_detail.csv"),
        results,
        ["code", "prefecture", "city_en", "city_jp",
         "pop_cagr", "under20_cagr", "age2039_cagr", "income_cagr",
         "score_pop", "score_under20", "score_2039", "score_income",
         "score_safety", "score_land_price", "score_land_cagr", "score_park",
         "macro_total", "station_score", "total_score"]
    )

    # Write summary total
    total_rows = [{"code": r["code"], "macro_score": r["macro_total"],
                   "station_score": r["station_score"], "total_score": r["total_score"]}
                  for r in results]
    write_csv(
        os.path.join(data_dir, "scores_total.csv"),
        total_rows,
        ["code", "macro_score", "station_score", "total_score"]
    )

    print(f"\n  Done. {len(results)} municipalities scored.")
    print(f"  Macro score range: {min(r['macro_total'] for r in results)} - {max(r['macro_total'] for r in results)}")
    print(f"  Total score range: {min(r['total_score'] for r in results)} - {max(r['total_score'] for r in results)}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="ASTRO Heatmap — e-Stat data fetcher & scorer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Step 1: Discover available tables
  python fetch_estat.py --appid YOUR_ID --discover

  # Step 2: Fetch data and compute scores
  python fetch_estat.py --appid YOUR_ID --fetch

  # Recompute scores from existing CSVs (no API needed)
  python fetch_estat.py --recompute --data-dir ./data
        """
    )
    parser.add_argument("--appid", help="e-Stat API application ID")
    parser.add_argument("--discover", action="store_true", help="Search for relevant e-Stat tables")
    parser.add_argument("--fetch", action="store_true", help="Fetch data and compute scores")
    parser.add_argument("--recompute", action="store_true", help="Recompute scores from existing CSVs")
    parser.add_argument("--data-dir", default="./data", help="Path to data directory (default: ./data)")

    args = parser.parse_args()

    if args.recompute:
        recompute_from_existing(
            args.data_dir,
            safety_data={},
            station_data={}
        )
    elif args.discover:
        if not args.appid:
            print("ERROR: --appid required for --discover")
            sys.exit(1)
        discover_tables(args.appid)
    elif args.fetch:
        if not args.appid:
            print("ERROR: --appid required for --fetch")
            sys.exit(1)
        fetch_and_export(args.appid, args.data_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
