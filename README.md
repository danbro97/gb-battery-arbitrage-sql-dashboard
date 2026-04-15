# GB Battery Arbitrage Opportunity Explorer

A compact portfolio project estimating the gross energy arbitrage opportunity for a GB battery using public half-hourly power market data, SQL modelling, Python dispatch logic, and a Power BI dashboard.

## Project aim

The project answers a simple commercial question:

> Under transparent operating assumptions, what gross arbitrage revenue opportunity would a GB battery have seen over calendar year 2025?

The analysis focuses on how battery value varies by:

- battery duration
- round-trip efficiency
- month and season
- weekday versus weekend
- daily price shape
- negative price periods
- intraday price spread

## Why this project

This project is designed for power and energy market analyst roles. It demonstrates the ability to:

- ingest public GB power market data
- clean and validate half-hourly time-series data
- model analytical tables in SQL
- implement battery dispatch calculations in Python
- create dashboard outputs and communicate commercial results in Power BI


## Data source

The price data uses Elexon Market Index Data for calendar year 2025.

The raw data is pulled from the public Elexon API in 7-day chunks, then combined into a cleaned APXMIDP price series.

Final cleaned price table:

- 17,520 half-hourly periods
- 365 settlement days
- no missing half-hours after cleaning
- duplicate API boundary rows removed

Raw and processed data files are excluded from Git using `.gitignore`.

## Battery assumptions

Base modelling assumptions:

- Geography: Great Britain
- Analysis period: calendar year 2025
- Time resolution: half-hourly
- Battery power: 1 MW
- Durations tested: 1h, 2h, 4h
- Round-trip efficiencies tested: 85%, 90%, 92.5%
- Max cycles per day: 1
- Revenue basis: gross energy arbitrage only

The dispatch model selects one contiguous charge block and one later contiguous discharge block within each day. If no profitable pair exists, daily revenue is set to zero.

## Exclusions

The model does not include:

- ancillary services
- degradation costs
- network charges
- trading fees
- imbalance risk
- forecast uncertainty
- battery availability constraints
- network or connection constraints

This keeps the project focused on price shape-driven arbitrage opportunity.

## Tools used

- Python
- pandas
- NumPy
- DuckDB
- SQL
- API
- Power BI
- Git / GitHub

## Repository structure

```text
gb-battery-arbitrage-sql-dashboard/
├─ dashboard/
│  └─ gb_battery_arbitrage.pbix
├─ data_processed/
├─ data_raw/
├─ db/
├─ sql/
│  ├─ 01_create_schemas.sql
│  ├─ 02_load_prices.sql
│  ├─ 03_build_price_mart.sql
│  └─ 04_build_battery_summary_tables.sql
├─ src/
│  ├─ build_battery_summary_tables.py
│  ├─ build_daily_price_shape_features.py
│  ├─ build_price_mart.py
│  ├─ export_dashboard_data.py
│  ├─ ingest_prices.py
│  ├─ load_prices_to_duckdb.py
│  └─ run_battery_scenarios.py
├─ .gitignore
├─ README.md
└─ requirements.txt
```

## Pipeline

Run the project pipeline in this order:

```bash
python src/ingest_prices.py
python src/load_prices_to_duckdb.py
python src/build_price_mart.py
python src/build_daily_price_shape_features.py
python src/run_battery_scenarios.py
python src/build_battery_summary_tables.py
python src/export_dashboard_data.py
```

The pipeline:

1. downloads Elexon Market Index Data in valid 7-day API chunks
2. combines and cleans the APXMIDP price series
3. loads cleaned prices into DuckDB
4. builds half-hourly and daily analytical tables
5. runs battery dispatch scenarios
6. creates annual, monthly, and day-type summary tables
7. exports dashboard-ready CSVs for Power BI

## Dashboard

The Power BI dashboard contains three pages.

### 1. Executive Overview

Shows selected-scenario KPIs:

- annual gross revenue
- average daily gross revenue
- profitable days
- monthly revenue profile
- seasonal and day-type revenue breakdown

### 2. Value Drivers

Explores what produces battery value:

- daily intraday spread versus battery revenue
- top 15 value days
- average daily revenue by count of negative-price half-hours

### 3. Scenario Sensitivity

Compares revenue outcomes across:

- 1h, 2h, and 4h battery durations
- 85%, 90%, and 92.5% round-trip efficiency

## Key output

For the base case of a **1 MW / 2 MWh battery** with **90% round-trip efficiency** and **one cycle per day**, the model estimates annual gross arbitrage revenue of approximately:

**£39k in 2025**

This is gross revenue only.

## Interpretation

The results show that battery arbitrage value is strongly linked to daily price shape. Days with wider intraday spreads produce higher value, while longer charge duration and higher efficiency scenarios capture more revenue.

The dashboard also shows that value is uneven across the year, making seasonality and market conditions important for interpreting battery opportunity.

## Limitations

This project is a transparent analytical model, not a trading strategy or full asset valuation model.

The results are useful for understanding price-shape opportunity, but a real battery revenue model would need to include:

- degradation
- trading fees
- network charges
- battery availability
- market access constraints
- forecasting error
- imbalance exposure
- ancillary service stacking
- operational constraints

## Portfolio summary

This project demonstrates SQL-based energy market data modelling, Python scenario analysis, and Power BI dashboarding for a commercially relevant GB power market use case.