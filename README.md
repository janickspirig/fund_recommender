# IF Recommender - Brazilian Fixed Income Fund Recommendation System


A **prototype** data pipeline that analyze Brazilian fixed income funds and provides recommendations for different investor profiles.

## Features

- **7 Risk/Return Metrics**: Volatility, Sharpe ratio, liquidity, concentration, asset diversification, fund age, credit quality
- **4 pre-set Investor Profiles**: Conservative, Moderate, Aggressive, Speculator with option to add additional ones easily
- **Brazilian Market Data**: CVM regulatory filings + ANBIMA fund characteristics

## Quick Start

### Prerequisites

- Python 3.9+
- pip

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd if-recomender
   ```

2. **Create and activate virtual env (conda, pyenv etc.)**

3. **Install dependencies**
   ```bash
   make install
   # or: pip install -r requirements.txt
   ```

4. **Download raw data**: Provided separately via [Google Drive](https://drive.google.com/drive/u/0/folders/1MIdHlpL7NkzWolrjW1f1rtbr_WePDhnW)
   ```bash
   # Download data.zip from GDrive
   # Extract into project root inside /data folder
   unzip 01_raw.zip
   ```

5. **Run the pipeline**
   ```bash
   make run
   # or: kedro run
   ```

6. **View recommendations**
   ```bash
   cat data/08_reporting/rpt_shortlist.csv
   ```

7. **Visualize pipeline** (optional)
   ```bash
   kedro viz
   # Opens interactive pipeline visualization at http://127.0.0.1:4141
   ```

## Project Structure

```
if-recomender/
├── conf/
│   └── base/
│       ├── catalog.yml      # Data sources & outputs
│       ├── globals.yml      # Global computed values (max_period, max_ref_date, trading_days)
│       └── parameters.yml   # Configuration (investor profiles, thresholds, guardrails)
├── data/
│   ├── 01_raw/
│   │   ├── anbima/          # ANBIMA fund characteristics
│   │   └── cvm/data/
│   │       ├── monthly/     # Monthly CVM data
│   │       │   ├── blc_1/ through blc_8/  # Portfolio holdings by asset type
│   │       │   └── pl/      # Monthly NAV data
│   │       └── daily/
│   │           └── quotas/  # Daily quota files (inf_diario_fi)
│   ├── 01_raw_backup/       # Timestamped backups of raw data
│   ├── 02_intermediate/     # Normalized & filtered data
│   ├── 03_primary/          # Clean tables (daily returns, characteristics)
│   ├── 04_feature/          # Calculated features (volatility, Sharpe, etc.)
│   ├── 05_model_input/      # Normalized scoring inputs
│   ├── 07_model_output/     # Fund scores per profile & guardrail results
│   └── 08_reporting/        # Final recommendations
├── scripts/
│   ├── download_fi_daily_data.py  # Download daily quota data from CVM
│   └── restore_data.py            # Restore raw data from backup
├── src/if_recomender/
│   ├── nodes/
│   │   ├── int/             # Data normalization & filtering
│   │   ├── pri/             # Primary tables (daily returns, characteristics)
│   │   ├── feat/            # Feature calculation
│   │   ├── mi/              # Score normalization
│   │   ├── mo/              # Profile scoring & guardrails
│   │   └── rpt/             # Reporting & rankings
│   └── pipelines/           # Kedro pipelines
└── notebooks/               # Jupyter notebooks for analysis
```

## Data Sources

### CVM (Comissão de Valores Mobiliários)
- **Daily fund quota values** (inf_diario_fi files) - used for return/volatility calculations
- Monthly fund NAV data (PL files) - used for fund filtering and scope determination
- Portfolio holdings by asset type (BLC_1 through BLC_8)
- Period: Jan 2023 - Sep 2025

### ANBIMA (Brazilian Financial Markets Association)
- Fund characteristics (redemption terms, categories, inception dates)
- ~6,000 fixed income funds

## Pipeline Overview

```
Raw Data → Filter FI Funds → Calculate Returns → Compute Features 
→ Normalize Scores → Rank by Profile → Top 5 Recommendations
```

**Pipelines**:
1. **Intermediate**: Filter fixed income funds from raw data
2. **Primary**: Create NAV, returns, characteristics tables
3. **Feature**: Calculate 7 features, merge into unified table
4. **Model Input**: Normalize features to 0-1 scores
5. **Model Output**: Score funds per investor profile
6. **Reporting**: Generate top N recommendations

## Key Features

### 1. Volatility & Sharpe Ratio
- Calculated from **daily quota returns** (VL_QUOTA), not monthly NAV
- 252 trading days annualization convention
- Fixed trading-day windows: 252 days (12m), 63 days (3m)
- Sharpe ratio uses pre-calculated annualized volatility from volatility feature

### 2. Liquidity
- Based on redemption payment days
- D+0 (same-day) = highest score

### 3. Concentration (HHI)
- Herfindahl-Hirschman Index
- Instrument-level diversification

### 4. Asset Diversification
- Category-level HHI
- Spread across 8 asset classes
- Current holdings only

### 5. Credit Quality
- Brazilian rating scale (BRAAA, AAA(BR, etc.)
- Investment grade threshold
- Private credit focus

### 6. Fund Age
- Track record / maturity
- Capped at 30 years (adjustable)

## Configuration

All parameters are defined in `conf/base/parameters.yml`.

### Core Parameters

**n_top_funds_output** (integer, default: 5)
- Number of top recommendations per investor profile

**risk_free_rate_annual** (float, 0-1, default: 0.1371)
- Annual risk-free rate for Sharpe calculation (13.71% CDI)

**normalization_lower_percentile** (float, 0-1, default: 0.05)
**normalization_upper_percentile** (float, 0-1, default: 0.95)
- Percentile bounds for feature normalization

**fund_age_cap_years** (float, default: 30.0)
- Maximum years for fund age scoring

**min_data_period_per_fund** (integer, default: 3)
- Minimum periods required for fund inclusion

### Investor Profiles

Define custom profiles with weights that sum to 1.0:

```yaml
investor_profiles:
  your_profile_name:
    liquidity: 0.15           # 0-1, weight for redemption speed
    risk_reward: 0.40         # 0-1, weight for Sharpe ratio
    volatility: 0.15          # 0-1, weight for return stability
    concentration: 0.10       # 0-1, weight for diversification
    asset_diversification: 0.10  # 0-1, weight for asset spread
    fund_age: 0.05            # 0-1, weight for track record
    credit_quality: 0.05      # 0-1, weight for credit ratings
```

**Optional Profile Filters**:
- `allowed_fund_subtypes`: List of ANBIMA subtypes
- `target_investor_profile`: List of target investor types (e.g., "Profissional")

### Guardrails

Guardrails are post-scoring filters that ensure recommendation quality by excluding funds that don't meet minimum criteria. They run after scoring but before final ranking.

```yaml
guardrails:
  min_offer_per_issuer:           # Require fund manager to have multiple funds
    active: true
    params:
      min_offer_count: 5         

  min_threshold_sharpe_12m:       # Require 12-month positive Sharpe
    active: false                 # Disabled - conservative funds have negative Sharpe
    params:
      min_sharpe_12m: 0.0        

  min_threshold_sharpe_3m:        # Require 3-month positive Sharpe
    active: false
    params:
      min_sharpe_3m: 0.0

  min_threshold_cov_sharpe_12m:   # Require sufficient data coverage for 12m metrics
    active: true
    params:
      min_coverage_12m: 0.80      # 80% of trading days required

  min_threshold_cov_sharpe_3m:    # Require sufficient data coverage for 3m metrics
    active: true
    params:
      min_coverage_3m: 0.80       # 80% of trading days required

  no_funds_wo_manager:            # Exclude funds with unknown manager
    active: false

  include_only_active_funds:      # Exclude funds without recent daily quota data
    active: true

  no_extreme_returns:             # Exclude funds with extreme daily returns
    active: true
    params:
      max_abs_daily_return: 0.10  # 10% daily threshold                 
```

Failed guardrails are tracked in `mo_guardrail_mark.csv` with a `failed_guardrails` column showing which checks each fund failed.

### Data Scope

**num_period_months** (integer, default: null)
- Number of recent months to analyze
- When `null`, includes all available historical data from raw folder

**remove_funds_w_negative_cvm_pl_values** (boolean, default: true)
- Filters out funds with negative NAV values in latest period
- See Known Data Quality Issues for details

**cvm_fi_fund_types** (list)
- CVM fund types to include

**anbima_fi_fund_types** (list)
- ANBIMA categories to include

**anbima_accessability** (list)
- Investor accessibility restrictions

## Output

### Final Recommendations
**File**: `data/08_reporting/rpt_shortlist.csv`

**Columns**: CNPJ of Fund, Investor Profile, Rank, Score, Fund Name

**Example**:
```
CNPJ of Fund,Investor Profile,Rank,Score,Fund Name
12345000100,conservative,1,0.9562,COLÔNIA FIF RF CRÉDITO PRIVADO
67890000200,aggressive,1,0.9602,WESTERN ASSET GROWTH FIF
```


## Sample Results

> **Note**: Results vary based on data period and configuration. Run `kedro run` to generate current recommendations.

### Top 5 Funds per Profile

#### Conservative
| # | Fund Name | Score | Vol 12m | Sharpe 12m | Sharpe 3m |
|---|-----------|-------|---------|------------|-----------|
| 1 | SULAMÉRICA EXCLUSIVE FIF RF REFERENCIADO DI | 0.79 | 0.11% | -2.8 | 20.0 |
| 2 | BRADESCO FIF RF REFERENCIADA DI FEDERAL EXTRA | 0.79 | 0.09% | -6.7 | 20.0 |
| 3 | BB TOP RENDA FIXA CURTO PRAZO FIF | 0.78 | 0.09% | -5.8 | 20.0 |
| 4 | BB TOP PRINCIPAL RF REFERENCIADO DI LP FIF | 0.76 | 0.09% | -2.8 | 20.0 |
| 5 | ITAÚ RF CP FIF | 0.75 | 0.09% | -4.9 | 20.0 |

#### Moderate
| # | Fund Name | Score | Vol 12m | Sharpe 12m | Sharpe 3m |
|---|-----------|-------|---------|------------|-----------|
| 1 | PORTO SEGURO MASTER FIF RF CRÉD PRIV LP | 0.87 | 0.15% | 4.7 | 20.0 |
| 2 | A1 PÓS FIXADO FIF RF CRÉD PRIV | 0.86 | 0.37% | 5.4 | 20.0 |
| 3 | CAIXA FIDELIDADE PRIVATE FIF RF LP | 0.85 | 0.13% | -1.8 | 20.0 |
| 4 | TOP RF MIX CRED PRIV LP FIF | 0.85 | 0.29% | 0.6 | 8.6 |
| 5 | SANTANDER EQUILÍBRIO RF DI CRÉD PRIV FIF | 0.84 | 0.18% | 0.2 | 20.0 |

#### Aggressive
| # | Fund Name | Score | Vol 12m | Sharpe 12m | Sharpe 3m |
|---|-----------|-------|---------|------------|-----------|
| 1 | PORTO SEGURO MASTER FIF RF CRÉD PRIV LP | 0.88 | 0.15% | 4.7 | 20.0 |
| 2 | A1 PÓS FIXADO FIF RF CRÉD PRIV | 0.87 | 0.37% | 5.4 | 20.0 |
| 3 | ATENA FIF RF CRÉDITO PRIVADO | 0.83 | 0.08% | 3.5 | 20.0 |
| 4 | TOP RF MIX CRED PRIV LP FIF | 0.83 | 0.29% | 0.6 | 8.6 |
| 5 | KINEA FIF RF CRÉD PRIV | 0.83 | 0.37% | 2.7 | 10.5 |

#### Speculator
| # | Fund Name | Score | Vol 12m | Sharpe 12m | Sharpe 3m |
|---|-----------|-------|---------|------------|-----------|
| 1 | ATENA FIF RF CRÉDITO PRIVADO | 0.86 | 0.08% | 3.5 | 20.0 |
| 2 | ICATU VANGUARDA VEÍCULO ESPECIAL DC CRED PRIV | 0.84 | 0.19% | 8.5 | 20.0 |
| 3 | BB VIS CELESC FIC FIF RF LP | 0.83 | 0.09% | 3.8 | 20.0 |
| 4 | MAPFRE FIF RF II CRED PRIV | 0.83 | 0.14% | 2.8 | 13.0 |
| 5 | Minascoop FIF RF Crédito Privado | 0.82 | 0.30% | 1.8 | 20.0 |

**Key Observations**:
- **Conservative**: DI/Soberano funds with ultra-low volatility (~0.08-0.11%), negative 12m Sharpe (tracking CDI closely)
- **Moderate/Aggressive**: Credit Private funds with positive Sharpe 12m (0.6-5.4) and higher returns
- **Speculator**: Private credit funds with highest Sharpe 12m ratios (up to 8.5)
- **Sharpe capped at ±20**: Extreme values from ultra-low volatility funds are capped to avoid misleading metrics

---

## Known Data Quality Issues

### 1. Malformed Quotes in CVM Files

**Issue**: Some fund names contain unescaped quotes that break CSV parsing.

**Example** (`pl/202304.csv` line 3):
```csv
TP_FUNDO;CNPJ_FUNDO;DENOM_SOCIAL;DT_COMPTC;VL_PATRIM_LIQ
FACFIF;07.408.147/0001-64;FUNDO AMAZONIA... MIX "2";2023-04-30;22317.32
```

**Resolution**: Auto-fixed by validation hooks (quotes are doubled/escaped before pipeline run).

---

### 2. Redundant Quotes in CVM Files

**Issue**: Extra quote characters before delimiters cause parsing failures.

**Example** (`pl/202312.csv` line 23970):
```csv
TP_FUNDO;CNPJ_FUNDO;DENOM_SOCIAL;DT_COMPTC;VL_PATRIM_LIQ
FI;50.095.878/0001-26;DECK FUNDO DE INVESTIMENTO FINANCEIRO",;2023-12-31;8171565.36
```

**Resolution**: Auto-fixed by validation hooks (redundant quotes removed).

---

### 3. Negative NAV Values

**Issue**: Some funds report negative patrimonio liquido (NAV), which is anomalous.

**Example** (`pl/202306.csv`):
```csv
TP_FUNDO;CNPJ_FUNDO;DENOM_SOCIAL;DT_COMPTC;VL_PATRIM_LIQ
FI;10.705.306/0001-05;URCA FUNDO DE INVESTIMENTO RF...;2023-06-30;-184655.57
```

**Resolution**: Filtered via `remove_funds_w_negative_cvm_pl_values: true`. Affects ~210 funds with 948 negative NAV records across the dataset.

---

### 4. Missing Period Data

**Issue**: Some funds have gaps in their reporting periods (non-consecutive months).

**Example**: CNPJ 35755913000100 has 27 of 28 expected months (202301-202504), missing 202404.

**Resolution**: Returns calculated only between consecutive periods (null for gaps).

---

### 5. Extreme Returns from Capital Flows (RESOLVED)

**Issue**: Returns calculated from NAV changes (`VL_PATRIM_LIQ`) incorrectly capture capital flows (subscriptions/redemptions) as investment returns.

**Resolution**: 
- **Implemented**: Returns are now calculated from **daily quota values (`VL_QUOTA`)** from CVM inf_diario_fi files
- Quota values are normalized for capital flows and represent true investment performance
- `no_extreme_returns` guardrail checks daily returns with 10% threshold as additional safety net

---

## Limitations & Next Steps

**Current Limitations**:
- This is a prototype with simplified assumptions
- Data fetching is manual (no automated API integration)
- Parameter tuning is manual (no optimization)
- Limited to Brazilian fixed income funds

**Completed Enhancements**:
1. ~~**Use Quota Value for Returns**~~: **Implemented** - Returns are now calculated from daily `VL_QUOTA` values from CVM inf_diario_fi files, eliminating capital flow distortions

**Recommended Next Steps**:
1. **Automated Data Pipeline**: Integrate ANBIMA and CVM APIs with Kedro hooks to enable automatic data refresh before pipeline run
2. **File format**: Migrate to .parquet format or Databricks delta tables to enable scalable approach for longer historical data
3. **Feature Engineering**: Add duration, credit spread, manager performance, etc.
4. **Expand DataFrame validations**: Add more validations and leverage libraries like pandera, great expectations etc.
5. **Parameter Optimization**: Use Bayesian optimization or ML to learn optimal weights
6. **Backtesting**: Validate recommendations against historical performance
7. **Unit tests**: Add comprehensive unit tests with pytest to test logic of each pipeline node in isolation
8. **User Interface**: Build web dashboard for interactive fund exploration


