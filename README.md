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
│       ├── globals.yml      # Global computed values (max_period)
│       └── parameters.yml   # Configuration (investor profiles, thresholds)
├── data/
│   ├── 01_raw/              # Raw CVM & ANBIMA data (from Google Drive)
│   ├── 01_raw_backup/       # Timestamped backups of raw data
│   ├── 02_intermediate/     # Filtered & joined data
│   ├── 03_primary/          # Clean tables (NAV, returns, characteristics)
│   ├── 04_feature/          # Calculated features (unified table)
│   ├── 05_model_input/      # Normalized scoring inputs
│   ├── 07_model_output/     # Fund scores per profile
│   └── 08_reporting/        # Final recommendations
├── scripts/
│   └── restore_data.py      # Restore raw data from backup
├── src/if_recomender/
│   ├── nodes/
│   │   ├── int/             # Data filtering
│   │   ├── pri/             # Primary tables
│   │   ├── feat/            # Feature calculation
│   │   ├── mi/              # Score normalization
│   │   ├── mo/              # Profile scoring & guardrails
│   │   └── rpt/             # Reporting & rankings
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

  min_threshold_sharpe_12m:       # Require 12-month positive track record
    active: true
    params:
      min_sharpe_12m: 0.0        

  min_threshold_sharpe_3m:        # Require 3-month track record
    active: false
    params:
      min_sharpe_3m: 0.0

  no_funds_wo_manager:            # Exclude funds with unknown manager
    active: false

  include_only_active_funds:      # Exclude funds without recent data (PL)
    active: true                 
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

The following results demonstrate the profile differentiation achieved by the recommendation system.

### Profile Ladder Summary

| Profile | Avg Sharpe 12m | Avg Volatility | Avg Age | Primary Fund Types |
|---------|----------------|----------------|---------|-------------------|
| Conservative | 1.09 | 10.1% | 19.1y | Duração Baixa Soberano, Indexados |
| Moderate | 3.38 | 6.5% | 15.4y | Duração Média/Baixa Grau de Investimento |
| Aggressive | 5.48 | 6.1% | 9.7y | Duração Livre Crédito Livre, Grau de Investimento |
| Speculator | 4.52 | 12.8% | 3.3y | Crédito Livre, Investimento no Exterior |

**Key Observations**:
- **Fund Age decreases** from Conservative (19y) → Moderate (15y) → Aggressive (10y) → Speculator (3y)
- **Sharpe ratio increases** from Conservative (1.09) → Aggressive (5.48) - higher risk-adjusted returns
- **Speculator volatility** is highest (12.8%) with foreign investment exposure
- **Conservative funds** focus on sovereign/government bonds ("Soberano", "Indexados")

### Top 5 Funds per Profile

#### Conservative
| # | Fund Name | Score | Sharpe 12m | Vol | Age |
|---|-----------|-------|------------|-----|-----|
| 1 | SAFRA SOBERANO DI CLASSE DE INVESTIMENTO RF R | 0.803 | 0.90 | 10.1% | 16y |
| 2 | ITAÚ RF CP FIF RESP LIMITADA | 0.793 | 0.07 | 3.2% | 24y |
| 3 | SANTANDER TÍTULOS PÚBLICOS RENDA FIXA REFEREN | 0.774 | 1.10 | 3.7% | 19y |
| 4 | ITAÚ INSTITUCIONAL RF IRF M 1 FIF RESP LIMITA | 0.759 | 2.27 | 4.7% | 18y |
| 5 | CLASSE ÚNICA DE COTAS DO GRUPAL CASH FUNDO DE | 0.758 | 1.12 | 28.7% | 18y |

#### Moderate
| # | Fund Name | Score | Sharpe 12m | Vol | Age |
|---|-----------|-------|------------|-----|-----|
| 1 | BB TOP RENDA FIXA ARROJADO FUNDO DE INVESTIME | 0.897 | 1.26 | 2.1% | 26y |
| 2 | VALORA ABSOLUTE FIF RF CRED PRIV LP - RESP LI | 0.896 | 5.79 | 6.0% | 17y |
| 3 | CLASSE ÚNICA DE COTAS DO RIZA LOTUS PLUS MAST | 0.880 | 3.20 | 8.2% | 4y |
| 4 | NU YIELD FIF RF CRED PRIV LP RESP LIMITADA | 0.880 | 5.04 | 9.7% | 4y |
| 5 | BRADESCO CLASSE DE INVESTIMENTO RF REFERENCIA | 0.879 | 1.60 | 6.3% | 26y |

#### Aggressive
| # | Fund Name | Score | Sharpe 12m | Vol | Age |
|---|-----------|-------|------------|-----|-----|
| 1 | VALORA ABSOLUTE FIF RF CRED PRIV LP - RESP LI | 0.926 | 5.79 | 6.0% | 17y |
| 2 | Sicoob DI Fundo de Investimento Financeiro Re | 0.914 | 4.22 | 0.9% | 14y |
| 3 | NU YIELD FIF RF CRED PRIV LP RESP LIMITADA | 0.906 | 5.04 | 9.7% | 4y |
| 4 | CLASSE ÚNICA DE COTAS DO ABSOLUTE CRETA MASTE | 0.905 | 8.61 | 9.4% | 3y |
| 5 | BNP PARIBAS RUBI CLASSE DE INVESTIMENTO EM CO | 0.903 | 3.71 | 4.6% | 11y |

#### Speculator
| # | Fund Name | Score | Sharpe 12m | Vol | Age |
|---|-----------|-------|------------|-----|-----|
| 1 | BRADESCO MASTER ULTRA PREVIDÊNCIA FI FINANCEI | 0.951 | 4.62 | 8.1% | 4y |
| 2 | ITAÚ JANEIRO OFF PREV RF IE FIF RESP LIMITADA | 0.899 | 7.16 | 23.7% | 2y |
| 3 | JGP CRÉDITO PV FIF RF IE - RESP LIMITADA | 0.899 | 3.71 | 8.8% | 6y |
| 4 | CAIXA MASTER II CLASSE DE INVESTIMENTO FINANC | 0.877 | 4.83 | 16.3% | 2y |
| 5 | ICATU VANGUARDA VEÍCULO ESPECIAL FUNDO DE INV | 0.840 | 2.25 | 6.8% | 3y |

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

**Resolution**: Filtered via `remove_funds_w_negative_cvm_pl_values: true` (3 funds, 14 records).

---

### 4. Missing Period Data

**Issue**: Some funds have gaps in their reporting periods (non-consecutive months).

**Example**: CNPJ 35755913000100 has 25 of expected 33 months between 202302-202504.

**Resolution**: Returns calculated only between consecutive periods (null for gaps).

---

## Limitations & Next Steps

**Current Limitations**:
- This is a prototype with simplified assumptions
- Data fetching is manual (no automated API integration)
- Parameter tuning is manual (no optimization)
- Limited to Brazilian fixed income funds

**Recommended Enhancements**:
1. **Automated Data Pipeline**: Integrate ANBIMA and CVM APIs with Kedro hooks to enable automatic data refresh before pipeline run

2. **File format**: Migrate to .parquet format or Databricks delta tables to enable scalable approach for longer historical data
3. **Feature Engineering**: Add duration, credit spread, manager performance, etc.
4. **Parameter Optimization**: Use Bayesian optimization or ML to learn optimal weights
5. **Backtesting**: Validate recommendations against historical performance
6. **Unit tests**: Add comprehensive unit tests with pytest to test logic of each pipeline node in isolation
7. **User Interface**: Build web dashboard for interactive fund exploration
8. ...


