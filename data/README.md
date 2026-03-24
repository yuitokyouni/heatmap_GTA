# ASTRO Heatmap 2026 — Data

Municipality scoring data for the ASTRO residential fund, covering ~300 municipalities across 5 Kantō prefectures (東京都, 神奈川県, 埼玉県, 千葉県, 茨城県).

## File structure

### Scores (output)
| File | Description |
|------|-------------|
| `scores_total.csv` | Final scores: Macro + Station = Total (300 municipalities) |
| `scores_macro_detail.csv` | Per-indicator raw values AND 1-10 scores, plus Macro total (300 rows) |

### Raw data — Macro indicators
| File | Source | Description |
|------|--------|-------------|
| `raw_population_10y.csv` | 住民基本台帳 | Total population 2015–2025, 10Y CAGR |
| `raw_under20_2039.csv` | 住民基本台帳 | Under-20 and 20–39 population 2015–2025, 10Y CAGR |
| `raw_single_household.csv` | 国勢調査 | Single-person households (under 64) 2015 vs 2020, growth rate |
| `raw_taxable_income.csv` | 市町村税課税状況等 | Taxable income 2017–2024, 7Y CAGR |
| `raw_crime.csv` | 警察庁 | Crime counts 2023–2025 |
| `raw_park_space.csv` | 都市公園整備 | Park area per capita (㎡/person) |
| `raw_land_price.csv` | 国土交通省 公示地価 | Land prices 2015–2024, 10Y/5Y CAGR |

### Raw data — Station indicators
| File | Source | Description |
|------|--------|-------------|
| `raw_station.csv` | 駅データ.jp + Google Maps | Station density, ETA to central Tokyo (209 municipalities with rail access) |

### Scoring methodology
| File | Description |
|------|-------------|
| `scoring_thresholds.csv` | Percentile breakpoints for Macro sub-indicators (1–10 scale) |
| `scoring_thresholds_station.csv` | Percentile breakpoints for Station sub-indicators |

## Scoring formula

```
Total Score = Macro Score × 1 + Station Score × 5

Macro Score (max 50) = Σ(sub-indicator scores × coefficient)
  - Population 10Y CAGR      (coeff: 1, score 1-10)
  - Single HH growth          (coeff: 0, currently excluded)
  - Under-20 pop CAGR         (coeff: 1, score 1-10)
  - 20-39 pop CAGR            (coeff: 1, score 1-10)
  - Taxable income CAGR       (coeff: 1, score 1-10)
  - Safety (crimes/1000 ppl)  (score 1-10, inverted)
  - Park space per capita     (score 1-10)

Station Score (max 50) = density_score × 0.3 + eta_score × 0.7
  - Station density (stations/km²)
  - ETA to central Tokyo terminal stations (min)

Municipalities without rail access: Total = Macro × 1.5
```

## Code format

`code` uses the 5-digit municipal code (全国地方公共団体コード) without check digit.
- `08201` = 水戸市, `13101` = 千代田区, `14133` = 川崎市中原区
