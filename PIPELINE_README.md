# ASTRO Heatmap — Data Pipeline

Automated data fetching and scoring for the ASTRO residential fund municipality heatmap.

## Setup

```bash
pip install requests  # only stdlib is used, but requests is nicer
```

Get a free e-Stat API key:
1. Go to https://www.e-stat.go.jp/api/
2. Register (ユーザ登録)
3. マイページ → アプリケーションID → 発行

## Usage

### Step 1: Discover e-Stat table IDs
```bash
python fetch_estat.py --appid YOUR_APPID --discover
```
This searches e-Stat for the relevant statistical tables and prints their IDs.

### Step 2: Fetch data & recompute scores
```bash
python fetch_estat.py --appid YOUR_APPID --fetch
```
Fetches fresh data from e-Stat API and recomputes all Macro scores.

### Step 3 (offline): Recompute scores from existing CSVs
```bash
python fetch_estat.py --recompute --data-dir ./data
```
No API key needed. Recomputes scores using the existing `data/raw_*.csv` files.
Useful for recalibrating weights or thresholds without re-fetching data.

## What's automated vs manual

| Indicator | Source | Auto? |
|-----------|--------|-------|
| Population 10Y CAGR | e-Stat (住民基本台帳) | ✓ |
| Under-20 pop CAGR | e-Stat (住民基本台帳) | ✓ |
| 20-39 pop CAGR | e-Stat (住民基本台帳) | ✓ |
| Taxable income CAGR | e-Stat (課税状況等の調) | ✓ |
| Crime rate | 各県警 (PDF/Excel) | ✗ manual |
| Station density | 駅データ.jp | ✗ stable |
| ETA to Tokyo | Google Directions API | ✓ (paid ~$1) |

## Repository structure

```
heatmap_GTA/
├── index.html              ← Dashboard (choropleth + station overlay)
├── fetch_estat.py          ← This script
├── data/
│   ├── raw_population_10y.csv
│   ├── raw_under20_2039.csv
│   ├── raw_taxable_income.csv
│   ├── raw_crime.csv           ← manual update
│   ├── raw_station.csv         ← stable
│   ├── raw_park_space.csv      ← manual update
│   ├── raw_land_price.csv      ← manual update
│   ├── scores_macro_detail.csv ← auto-generated
│   ├── scores_total.csv        ← auto-generated
│   ├── scoring_thresholds.csv
│   └── README.md
├── LICENSE
└── README.md
```
