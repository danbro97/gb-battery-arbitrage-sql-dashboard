# GB Battery Arbitrage Opportunity Explorer

## Project aim
Estimate the gross energy arbitrage revenue opportunity for a stylised GB battery over calendar year 2025 using public half-hourly power market data, SQL-based data modelling, a simple Python dispatch engine, and a Power BI dashboard.

## Core questions
- Under simple operating assumptions, what gross revenue opportunity would a GB battery have seen in 2025?
- How does that vary by month, season, and day type?
- How sensitive is it to round-trip efficiency, duration, and cycle assumptions?
- What price shapes and market conditions produce the most battery value?

## Scope
- Geography: Great Britain
- Time resolution: half-hourly
- Analysis period: calendar year 2025
- Revenue basis: gross arbitrage only
- Public data only

## Battery assumptions
- Base case: 1 MW / 2 MWh
- Durations tested: 1h, 2h, 4h
- Round-trip efficiency: 85%, 90%, 92.5%
- Max daily cycles: 1 and 2

## Exclusions
- No ancillary services
- No degradation costs
- No fees or losses beyond round-trip efficiency
- No forecast model
- No stochastic optimisation
- No network constraints

## Tools
- DuckDB for SQL data modelling
- Python for ingestion and dispatch calculation
- Power BI for dashboarding

## Planned workflow
1. Ingest and clean raw data
2. Build analytical tables in DuckDB
3. Run battery dispatch scenarios
4. Export scenario outputs for dashboarding
5. Build Power BI dashboard
6. Summarise findings in GitHub-ready documentation