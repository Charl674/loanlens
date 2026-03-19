# LoanLens 2.0 — Live Credit Risk Intelligence

A production-style analytics engineering project that ingests live Federal Reserve credit risk data, transforms it through a modern cloud data stack, and delivers a board-ready dashboard that updates automatically every quarter.

## Live Dashboard
[LoanLens — Live Credit Risk Intelligence](https://lookerstudio.google.com/s/gjyVuwWzDnk)

## Architecture
FRED API → Python → Snowflake Internal Stage → COPY INTO → dbt Core → Looker Studio

## Stack
- **Data Source:** FRED API — Federal Reserve Economic Data
- **Ingestion:** Python 3.11 — requests, pandas, snowflake-connector-python
- **Warehouse:** Snowflake — RAW / STAGING / MARTS schemas
- **Transformation:** dbt Core — 1 staging model, 4 mart models, 6 data tests
- **Visualisation:** Google Looker Studio — 4 dashboard views

## Data
8 Federal Reserve credit risk series from 2000 to present:
- Delinquency rates — Consumer, Business, Credit Cards, Commercial Real Estate, Mortgages
- Charge-off rates — Business Loans, Credit Cards
- Federal Funds Rate — macro policy indicator

## dbt Models
| Model | Type | Description |
|---|---|---|
| stg_delinquency_rates | View | Cleans and types raw FRED data |
| mart_delinquency_trends | Table | Delinquency rates over time with QoQ and YoY changes |
| mart_charge_off_analysis | Table | Charge-off vs delinquency spread by loan category |
| mart_macro_risk_correlation | Table | Fed Funds Rate correlation with credit risk metrics |
| mart_risk_scorecard | Table | RAG status scoring per loan category per quarter |

## Setup
1. Clone the repo
2. Create a `.env` file with your credentials
3. Run `pip install requests pandas snowflake-connector-python python-dotenv`
4. Run `python scripts/fetch_fred_data.py`
5. Run `cd loanlens_dbt && dbt run`

## Author
Analytics Engineer — credit data, lender validation, Snowflake, dbt, Python
