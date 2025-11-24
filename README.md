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

## Project Structure

```
if-recomender/
├── conf/
│   └── base/
│       ├── catalog.yml      # Data sources & outputs
│       └── parameters.yml   # Configuration (investor profiles, thresholds)
├── data/
│   ├── 01_raw/              # Raw CVM & ANBIMA data (from Google Drive)
│   ├── 02_intermediate/     # Filtered & joined data
│   ├── 03_primary/          # Clean tables (NAV, returns, characteristics)
│   ├── 04_feature/          # Calculated features (unified table)
│   ├── 05_model_input/      # Normalized scoring inputs
│   ├── 07_model_output/     # Fund scores per profile
│   └── 08_reporting/        # Final recommendations
├── src/if_recomender/
│   ├── nodes/
│   │   ├── int/             # Data filtering
│   │   ├── pri/             # Primary tables
│   │   ├── feat/            # Feature calculation
│   │   ├── mi/              # Score normalization
│   │   ├── mo/              # Profile scoring
│   │   └── reporting/       # Top recommendations
│   └── pipelines/           # Kedro pipelines
└── notebooks/               # Jupyter notebooks for analysis
```

## Data Sources

### CVM (Comissão de Valores Mobiliários)
- Monthly fund NAV data (PL files)
- Portfolio holdings by asset type (BLC_1 through BLC_8)
- Period: Jan 2023 - Sep 2025 (33 months)

### ANBIMA (Brazilian Financial Markets Association)
- Fund characteristics (redemption terms, categories, inception dates)
- ~5,400 fixed income funds

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
- 12-month rolling window
- Annualized metrics
- Risk-adjusted returns

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
- Capped at 20 years (adjustable)

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

**fund_age_cap_years** (float, default: 20.0)
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

```yaml
guardrails:
  min_offer_count_provider: 5  # Min funds per manager
  exclude_funds_without_manager: false  # Exclude unassigned funds
```

### Data Scope

**num_period_months** (integer, default: 33)
- Number of recent months to analyze

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


## Known Data Quality Issues

### 1. Concentration Metrics: HHI > 1.0

**Issue**: ~20% of funds had concentration HHI > 1.0 (max should be 1.0)

**Root Cause**: CVM's `blc_8` (OtherAssets) includes accounting entries (Valores a Pagar/Receber, Disponibilidade) that inflated position values beyond NAV.

**Resolution**: 
- Filter accounting entries via `blc_8_accounting_entry_patterns` in `parameters.yml`
- Guardrail: Automatically set HHI > 1.0 to `null` in concentration/diversification calculations
- Affected funds still scored with renormalized weights on remaining features

---

### 2. Monthly Returns Calculation

**Issue**: 
- Simple lag caused multi-month return jumps when funds skipped reporting periods
- Oldest month lost (lag calculated after filtering to N-month window)

**Resolution**:
- Left join with previous calendar month's NAV (handles missing months as null)
- Apply N-month filter AFTER calculating returns (preserves all valid data points)

---

## Limitations & Next Steps

**Current Limitations**:
- This is a prototype with simplified assumptions
- Data fetching is manual (no automated API integration)
- Parameter tuning is manual (no optimization)
- Limited to Brazilian fixed income funds

**Recommended Enhancements**:
1. **Automated Data Pipeline**: Integrate ANBIMA and CVM APIs with Kedro hooks to enable automatic data refresh before pipeline run
2. **Data Quality**: Improve CVM file parsing and handle edge cases of data quality issues / inconsistencies
3. **File format**: Migrate to .parquet format or Databricks delta tables to enable scalable approach for longer historical data
4. **Feature Engineering**: Add duration, credit spread, manager performance, etc.
5. **Parameter Optimization**: Use Bayesian optimization or ML to learn optimal weights
6. **Backtesting**: Validate recommendations against historical performance
7. **Unit tests**: Add comprehensive unit tests with pytest to test logic of each pipeline node in isolation
8. **User Interface**: Build web dashboard for interactive fund exploration
9. ...

