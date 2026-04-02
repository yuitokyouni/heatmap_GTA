"""
ASTRO Heatmap 2026 — Dashboard Build Pipeline
===============================================
Reads raw CSVs → computes percentile-based scores → generates index.html

Usage:
    python build_dashboard.py                    # default: data/ → index.html
    python build_dashboard.py --data-dir ./data  # custom data path
    python build_dashboard.py --dynamic-thresholds  # recompute percentile breakpoints from data

Flow:
    1. Load raw CSVs (population, age groups, income, crime, station)
    2. For each indicator: compute CAGR if needed, then score 1-10
    3. Aggregate: Macro = pop + under20 + age2039 + income + safety
    4. Total = Macro + Station×5  (no station: Macro × 1.5)
    5. Merge scores into GeoJSON properties
    6. Inject GeoJSON + scores + stations into template.html → index.html
"""

import argparse
import csv
import json
import math
import os
import sys
from pathlib import Path
from datetime import datetime
import hashlib

SCRIPT_DIR = Path(__file__).parent


# ============================================================
# 1. Code format helpers
# ============================================================

def to_dashboard_code(raw_code: str) -> str:
    """
    Convert municipal code to 5-digit dashboard format (no check digit).
    '82015' → '08201', '111007' → '11100', '08201' → '08201'
    """
    raw_code = str(raw_code).strip()
    if not raw_code:
        return raw_code
    # Remove .0 from float-like strings
    if raw_code.endswith('.0'):
        raw_code = raw_code[:-2]
    if not raw_code.isdigit():
        return raw_code
    c = int(raw_code)
    if raw_code.startswith('0'):
        return raw_code.zfill(5)
    if c > 14999:
        return str(c // 10).zfill(5)
    else:
        return str(c).zfill(5)


# ============================================================
# 2. CSV loading
# ============================================================

def load_csv(filepath: str) -> list:
    """Load CSV as list of dicts. Returns [] if file missing."""
    if not os.path.exists(filepath):
        print(f"  [WARN] File not found: {filepath}")
        return []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_raw_population(data_dir: str) -> dict:
    """Load population 10Y CAGR. Returns {dashboard_code: cagr_float}."""
    rows = load_csv(os.path.join(data_dir, "raw_population_10y.csv"))
    result = {}
    for row in rows:
        code = to_dashboard_code(row.get("code", ""))
        cagr = row.get("cagr_10y", "")
        if code and cagr:
            try:
                result[code] = float(cagr)
            except ValueError:
                pass
    print(f"  Population CAGR: {len(result)} municipalities")
    return result


def load_raw_age_groups(data_dir: str) -> dict:
    """Load under-20 and 20-39 CAGRs. Returns {code: {under20: float, age2039: float}}."""
    rows = load_csv(os.path.join(data_dir, "raw_under20_2039.csv"))
    result = {}
    for row in rows:
        code = to_dashboard_code(row.get("code", ""))
        if not code:
            continue
        entry = {}
        u20 = row.get("under20_cagr_10y", "")
        a2039 = row.get("age2039_cagr_10y", "")
        if u20:
            try:
                entry["under20"] = float(u20)
            except ValueError:
                pass
        if a2039:
            try:
                entry["age2039"] = float(a2039)
            except ValueError:
                pass
        if entry:
            result[code] = entry
    print(f"  Age group CAGRs: {len(result)} municipalities")
    return result


def load_raw_income(data_dir: str) -> dict:
    """Load taxable income CAGR. Returns {code: cagr_float}."""
    rows = load_csv(os.path.join(data_dir, "raw_taxable_income.csv"))
    result = {}
    for row in rows:
        code = to_dashboard_code(row.get("code", ""))
        cagr = row.get("cagr_7y", "")
        if code and cagr:
            try:
                result[code] = float(cagr)
            except ValueError:
                pass
    print(f"  Income CAGR: {len(result)} municipalities")
    return result


def load_raw_crime(data_dir: str) -> dict:
    """
    Load crime data → compute crimes per 1000 people.
    Returns {code: crimes_per_1000}.
    Falls back to raw crime count if population not available.
    """
    rows = load_csv(os.path.join(data_dir, "raw_crime.csv"))
    # Also need population for per-capita calculation
    pop_rows = load_csv(os.path.join(data_dir, "raw_population_10y.csv"))
    pop_latest = {}
    for row in pop_rows:
        code = to_dashboard_code(row.get("code", ""))
        # Use most recent year column
        for year_col in ["2025", "2024", "2023"]:
            val = row.get(year_col, "")
            if val:
                try:
                    pop_latest[code] = int(val)
                    break
                except ValueError:
                    pass

    result = {}
    for row in rows:
        code = to_dashboard_code(row.get("code", ""))
        if not code or int(code[:5].replace('0', '') or '0') < 82:
            continue
        # Use most recent crime count
        crimes = None
        for col in ["crimes_2025", "crimes_2024", "crimes_2023"]:
            val = row.get(col, "")
            if val:
                try:
                    crimes = int(float(val))
                    break
                except ValueError:
                    pass
        if crimes is not None and code in pop_latest and pop_latest[code] > 0:
            result[code] = crimes / pop_latest[code] * 1000
        elif crimes is not None:
            result[code] = crimes  # raw count as fallback

    print(f"  Crime rate: {len(result)} municipalities")
    return result


def load_raw_station(data_dir: str) -> dict:
    """Load station scores. Returns {dashboard_code: raw_score}."""
    rows = load_csv(os.path.join(data_dir, "raw_station.csv"))
    result = {}
    for row in rows:
        code = to_dashboard_code(row.get("code", ""))
        score = row.get("score", "")
        if code and score:
            try:
                result[code] = float(score)
            except ValueError:
                pass
    print(f"  Station scores: {len(result)} municipalities")
    return result


def load_existing_macro_detail(data_dir: str) -> dict:
    """Load existing scores_macro_detail.csv for supplementary fields (city names etc)."""
    rows = load_csv(os.path.join(data_dir, "scores_macro_detail.csv"))
    result = {}
    for row in rows:
        code = to_dashboard_code(row.get("code", ""))
        if code:
            result[code] = row
    return result


# ============================================================
# 3. Scoring engine
# ============================================================

# Default fixed thresholds (from original Excel model)
FIXED_THRESHOLDS = {
    "population": [-0.0714, -0.0171, -0.013435, -0.007223, -0.004017,
                   -0.001707, 0.000714, 0.002663, 0.004534, 0.006832, 0.030507],
    "under20":    [-0.072495, -0.038382, -0.03099, -0.025443, -0.020393,
                   -0.016199, -0.013462, -0.009486, -0.005583, 0.002955, 0.044285],
    "age2039":    [-0.064206, -0.036192, -0.027127, -0.021737, -0.016008,
                   -0.010901, -0.008229, -0.005191, -0.000487, 0.005208, 0.025557],
    "income":     [-0.0672, 0.001797, 0.007995, 0.011961, 0.015165,
                    0.019306, 0.023589, 0.025901, 0.030076, 0.033488, 0.101601],
    "safety":     [0.0, 3.9657, 4.5514, 5.095, 5.647, 6.1433, 6.4066, 6.8452, 7.4063, 8.3394, 43.074],
}

# Station scoring: raw_score × 5 = station contribution to total
STATION_MULTIPLIER = 5
NO_STATION_MACRO_MULTIPLIER = 1.5


def compute_percentile_thresholds(values: list, n_bins: int = 10) -> list:
    """
    Compute percentile-based thresholds from data.
    Returns list of n_bins+1 boundary values for scoring 1 to n_bins.
    """
    sorted_vals = sorted(v for v in values if v is not None)
    if len(sorted_vals) < n_bins:
        return None
    thresholds = []
    for i in range(n_bins + 1):
        idx = int(i / n_bins * (len(sorted_vals) - 1))
        thresholds.append(sorted_vals[idx])
    return thresholds


def score_value(value, thresholds: list) -> int:
    """Score a value 1-10 based on threshold breakpoints."""
    if value is None or thresholds is None:
        return None
    for i in range(len(thresholds) - 1):
        if value <= thresholds[i + 1]:
            return i + 1
    return 10


def score_value_inverted(value, thresholds: list) -> int:
    """Score inversely: lower value = higher score (for crime rate)."""
    s = score_value(value, thresholds)
    if s is None:
        return None
    return 11 - s  # 1→10, 2→9, ..., 10→1


def compute_all_scores(pop_data, age_data, income_data, crime_data, station_data,
                       existing_detail, dynamic_thresholds=False):
    """
    Main scoring function.
    Returns (results, thresholds).
    """
    # Collect all codes
    all_codes = sorted(set(
        list(pop_data.keys()) +
        list(age_data.keys()) +
        list(income_data.keys()) +
        list(crime_data.keys()) +
        list(existing_detail.keys())
    ))

    print(f"\n  Scoring {len(all_codes)} municipalities...")

    # Determine thresholds
    if dynamic_thresholds:
        print("  Computing dynamic percentile thresholds from data...")
        thresholds = {
            "population": compute_percentile_thresholds(list(pop_data.values())),
            "under20": compute_percentile_thresholds([v.get("under20") for v in age_data.values()]),
            "age2039": compute_percentile_thresholds([v.get("age2039") for v in age_data.values()]),
            "income": compute_percentile_thresholds(list(income_data.values())),
            "safety": compute_percentile_thresholds(list(crime_data.values())),
        }
        for key, t in thresholds.items():
            if t:
                print(f"    {key}: [{t[0]:.4f} ... {t[-1]:.4f}] ({len(t)-1} bins)")
            else:
                print(f"    {key}: insufficient data")
    else:
        print("  Using fixed thresholds from original Excel model...")
        thresholds = FIXED_THRESHOLDS.copy()
        # For safety, compute from data if not fixed
        if thresholds["safety"] is None:
            thresholds["safety"] = compute_percentile_thresholds(list(crime_data.values()))

    # Score each municipality
    results = []
    for code in all_codes:
        detail = existing_detail.get(code, {})

        # Raw values
        pop_cagr = pop_data.get(code)
        ages = age_data.get(code, {})
        u20_cagr = ages.get("under20")
        a2039_cagr = ages.get("age2039")
        inc_cagr = income_data.get(code)
        crime_rate = crime_data.get(code)

        # Individual scores (1-10)
        s_pop = score_value(pop_cagr, thresholds["population"])
        s_u20 = score_value(u20_cagr, thresholds["under20"])
        s_2039 = score_value(a2039_cagr, thresholds["age2039"])
        s_inc = score_value(inc_cagr, thresholds["income"])
        s_safety = score_value_inverted(crime_rate, thresholds["safety"])

        # Macro total = sum of available scores
        macro = 0
        for s in [s_pop, s_u20, s_2039, s_inc, s_safety]:
            if s is not None:
                macro += s

        # Station score
        stn_raw = station_data.get(code)
        stn_score = round(stn_raw * STATION_MULTIPLIER, 1) if stn_raw is not None else None

        # Total
        if stn_score is not None:
            total = macro + stn_score
        else:
            total = macro * NO_STATION_MACRO_MULTIPLIER

        results.append({
            "code": code,
            "prefecture": detail.get("prefecture", ""),
            "city_en": detail.get("city_en", ""),
            "city_jp": detail.get("city_jp", ""),
            "pop_cagr": pop_cagr,
            "under20_cagr": u20_cagr,
            "age2039_cagr": a2039_cagr,
            "income_cagr": inc_cagr,
            "crime_rate": crime_rate,
            "score_pop": s_pop,
            "score_under20": s_u20,
            "score_2039": s_2039,
            "score_income": s_inc,
            "score_safety": s_safety,
            "macro_total": macro,
            "station_raw": stn_raw,
            "station_score": stn_score,
            "total_score": round(total, 1),
        })

    return results, thresholds


# ============================================================
# 4. CSV output
# ============================================================

def write_scores_csv(results: list, data_dir: str):
    """Write scores_macro_detail.csv and scores_total.csv."""

    # Macro detail
    detail_fields = [
        "code", "prefecture", "city_en", "city_jp",
        "pop_cagr", "under20_cagr", "age2039_cagr", "income_cagr", "crime_rate",
        "score_pop", "score_under20", "score_2039", "score_income", "score_safety",
        "macro_total", "station_score", "total_score"
    ]
    detail_path = os.path.join(data_dir, "scores_macro_detail.csv")
    with open(detail_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=detail_fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            row = {}
            for k in detail_fields:
                v = r.get(k)
                if v is None:
                    row[k] = ""
                elif isinstance(v, float):
                    row[k] = f"{v:.6f}" if abs(v) < 1 else f"{v:.1f}"
                else:
                    row[k] = v
            w.writerow(row)
    print(f"  Written: {detail_path} ({len(results)} rows)")

    # Total summary
    total_path = os.path.join(data_dir, "scores_total.csv")
    with open(total_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["code", "macro_score", "station_score", "total_score"])
        for r in results:
            w.writerow([
                r["code"],
                r["macro_total"],
                r["station_score"] if r["station_score"] is not None else "",
                r["total_score"]
            ])
    print(f"  Written: {total_path} ({len(results)} rows)")


# ============================================================
# 5. Build score data for dashboard (JSON)
# ============================================================

# Prefecture code → short name mapping
PREF_MAP = {
    "08": "茨城", "11": "埼玉", "12": "千葉", "13": "東京", "14": "神奈"
}

def build_dashboard_data(results: list) -> str:
    """
    Convert score results into the compact JSON format used by the dashboard.
    Format: [{c, p, n, e, m, tr, to, la, lo, eta}, ...]
    """
    # Load coordinate data (from existing compact data if available)
    coord_path = SCRIPT_DIR / "geo" / "coordinates.json"
    coords = {}
    if coord_path.exists():
        with open(coord_path) as f:
            for entry in json.load(f):
                coords[entry["c"]] = entry

    # Also try loading from existing DATA in case coordinates.json doesn't exist
    # We'll build coordinates from the score detail + any existing source
    compact = []
    for r in results:
        code = r["code"]
        # Skip entries without a city name (county/prefecture-level aggregates)
        if not r["city_jp"]:
            continue
        pref_code = code[:2]
        pref_short = PREF_MAP.get(pref_code, "")

        entry = {
            "c": code,
            "p": pref_short,
            "n": r["city_jp"],
            "e": r["city_en"],
            "m": r["macro_total"],
            "tr": round(r["station_score"], 1) if r["station_score"] is not None else None,
            "to": r["total_score"],
            # Sub-scores for factor toggles
            "sp": r.get("score_pop") or 0,
            "su": r.get("score_under20") or 0,
            "sa": r.get("score_2039") or 0,
            "si": r.get("score_income") or 0,
            "ss": r.get("score_safety") or 0,
        }

        # Add coordinates if available
        if code in coords:
            c = coords[code]
            if "la" in c:
                entry["la"] = c["la"]
            if "lo" in c:
                entry["lo"] = c["lo"]
            if "eta" in c:
                entry["eta"] = c["eta"]

        compact.append(entry)

    return json.dumps(compact, ensure_ascii=False, separators=(",", ":"))


def short_file_hash(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()[:10]


def build_methodology_meta(args, results, thresholds) -> str:
    files = {
        "population": os.path.join(args.data_dir, "raw_population_10y.csv"),
        "age": os.path.join(args.data_dir, "raw_under20_2039.csv"),
        "income": os.path.join(args.data_dir, "raw_taxable_income.csv"),
        "crime": os.path.join(args.data_dir, "raw_crime.csv"),
        "station": os.path.join(args.data_dir, "raw_station.csv"),
    }

    versions = {}
    for key, path in files.items():
        if os.path.exists(path):
            versions[key] = short_file_hash(path)

    payload = {
        "build_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source_file_version": versions,
        "municipality_count": sum(1 for r in results if r.get("city_jp")),
        "score_thresholds_used": thresholds,
        "selected_methodology_label": (
            "Dynamic percentile thresholds"
            if args.dynamic_thresholds else
            "Fixed thresholds from original Excel model"
        ),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


# ============================================================
# 6. HTML generation
# ============================================================

def build_html(score_json: str, geo_path: str, station_path: str, template_path: str, meta_json: str) -> str:
    """
    Read template.html, inject GeoJSON + score data + station data + methodology metadata.
    """
    print("\n  Building index.html...")

    with open(template_path) as f:
        template = f.read()

    with open(geo_path) as f:
        geo_json = f.read()

    with open(station_path) as f:
        station_json = f.read()

    # Also need to merge score data INTO the GeoJSON properties
    # so the choropleth colors work
    scores_by_code = {}
    for entry in json.loads(score_json):
        scores_by_code[entry["c"]] = entry

    geo_data = json.loads(geo_json)
    for feature in geo_data["features"]:
        code = feature["properties"]["c"]
        if code in scores_by_code:
            s = scores_by_code[code]
            feature["properties"]["to"] = s["to"]
            feature["properties"]["m"] = s["m"]
            feature["properties"]["tr"] = s.get("tr")
            feature["properties"]["e"] = s.get("e", "")
            feature["properties"]["p"] = s.get("p", "")

    geo_json_merged = json.dumps(geo_data, ensure_ascii=False, separators=(",", ":"))

    # Inject into template
    html = template.replace("/*__GEO_DATA__*/null", geo_json_merged)
    html = html.replace("/*__SCORE_DATA__*/null", score_json)
    html = html.replace("/*__STATION_DATA__*/null", station_json)
    html = html.replace("/*__META_DATA__*/null", meta_json)

    # Inject zones data if available
    zones_path = SCRIPT_DIR / "secondary" / "zones.json"
    if zones_path.exists():
        with open(zones_path) as f:
            zones_data = f.read()
        html = html.replace("/*__ZONES_DATA__*/null", zones_data)
        print(f"    Zones:    {len(zones_data)/1024:.0f} KB")
    else:
        print(f"    Zones:    (not found, zone tabs disabled)")

    # Inject property data if available
    props_path = SCRIPT_DIR / "data" / "properties.json"
    if props_path.exists():
        with open(props_path, encoding="utf-8") as f:
            props_data = f.read()
        html = html.replace("/*__PROPERTY_DATA__*/null", props_data)
        print(f"    Properties: {len(props_data)/1024:.0f} KB")
    else:
        print(f"    Properties: (not found, no pins shown)")

    print(f"  index.html: {len(html)/1024:.0f} KB")
    print(f"    GeoJSON:  {len(geo_json_merged)/1024:.0f} KB")
    print(f"    Scores:   {len(score_json)/1024:.0f} KB")
    print(f"    Stations: {len(station_json)/1024:.0f} KB")

    return html


# ============================================================
# 7. Main CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="ASTRO Heatmap — build dashboard from raw CSVs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python build_dashboard.py                         # build with fixed thresholds
  python build_dashboard.py --dynamic-thresholds    # recompute percentiles from data
  python build_dashboard.py --data-dir ./data --output ./index.html
        """
    )
    parser.add_argument("--data-dir", default=str(SCRIPT_DIR / "data"),
                        help="Path to data directory (default: ./data)")
    parser.add_argument("--output", default=str(SCRIPT_DIR / "index.html"),
                        help="Output HTML path (default: ./index.html)")
    parser.add_argument("--dynamic-thresholds", action="store_true",
                        help="Recompute scoring thresholds from current data (vs fixed from Excel)")
    parser.add_argument("--geo-dir", default=str(SCRIPT_DIR / "geo"),
                        help="Path to geo data directory (default: ./geo)")
    parser.add_argument("--template", default=str(SCRIPT_DIR / "template.html"),
                        help="Path to HTML template (default: ./template.html)")

    args = parser.parse_args()

    print("=" * 60)
    print("ASTRO Heatmap — Dashboard Build Pipeline")
    print("=" * 60)

    # Step 1: Load raw data
    print("\n[Step 1] Loading raw data...")
    pop_data = load_raw_population(args.data_dir)
    age_data = load_raw_age_groups(args.data_dir)
    income_data = load_raw_income(args.data_dir)
    crime_data = load_raw_crime(args.data_dir)
    station_data = load_raw_station(args.data_dir)
    existing_detail = load_existing_macro_detail(args.data_dir)

    # Step 2: Compute scores
    print("\n[Step 2] Computing scores...")
    results, thresholds = compute_all_scores(
        pop_data, age_data, income_data, crime_data, station_data,
        existing_detail, dynamic_thresholds=args.dynamic_thresholds
    )

    # Step 3: Write CSVs
    print("\n[Step 3] Writing score CSVs...")
    write_scores_csv(results, args.data_dir)

    # Step 4: Build dashboard JSON
    print("\n[Step 4] Building dashboard data...")
    score_json = build_dashboard_data(results)

    # Step 5: Build methodology metadata
    print("\n[Step 5] Building methodology metadata...")
    meta_json = build_methodology_meta(args, results, thresholds)

    # Step 6: Generate HTML
    print("\n[Step 6] Generating index.html...")
    geo_path = os.path.join(args.geo_dir, "kanto.json")
    station_path = os.path.join(args.geo_dir, "stations.json")

    if not os.path.exists(geo_path):
        print(f"  [ERROR] GeoJSON not found: {geo_path}")
        sys.exit(1)
    if not os.path.exists(station_path):
        print(f"  [ERROR] Station data not found: {station_path}")
        sys.exit(1)
    if not os.path.exists(args.template):
        print(f"  [ERROR] Template not found: {args.template}")
        sys.exit(1)

    html = build_html(score_json, geo_path, station_path, args.template, meta_json)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    # Summary
    scored = [r for r in results if r["macro_total"] > 0]
    with_station = [r for r in results if r["station_score"] is not None]
    print(f"\n{'='*60}")
    print(f"BUILD COMPLETE")
    print(f"{'='*60}")
    print(f"  Municipalities scored: {len(scored)}")
    print(f"  With station data:     {len(with_station)}")
    print(f"  Macro range:           {min(r['macro_total'] for r in scored)} – {max(r['macro_total'] for r in scored)}")
    print(f"  Total range:           {min(r['total_score'] for r in scored)} – {max(r['total_score'] for r in scored)}")
    print(f"  Output:                {args.output}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()