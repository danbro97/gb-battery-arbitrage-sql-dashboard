import duckdb

con = duckdb.connect("db/gb_battery.duckdb")

con.execute("""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;
""")

con.execute("""
DROP TABLE IF EXISTS raw.mid_prices_2025;
""")

con.execute("""
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
""")

print(con.execute("""
SELECT COUNT(*) AS row_count,
       MIN(start_time) AS min_time,
       MAX(start_time) AS max_time,
       MIN(price_gbp_per_mwh) AS min_price,
       MAX(price_gbp_per_mwh) AS max_price
FROM raw.mid_prices_2025
""").fetchdf())

con.close()