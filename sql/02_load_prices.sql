DROP TABLE IF EXISTS raw.mid_prices_2025;

CREATE TABLE raw.mid_prices_2025 AS
SELECT
    CAST(startTime AS TIMESTAMP) AS start_time,
    CAST(settlementDate AS DATE) AS settlement_date,
    CAST(settlementPeriod AS INTEGER) AS settlement_period,
    CAST(price AS DOUBLE) AS price_gbp_per_mwh,
    CAST(volume AS DOUBLE) AS volume_mwh,
    dataProvider AS data_provider,
    source_file
FROM read_csv_auto('data_processed/analytical/mid_2025_apxmidp.csv');