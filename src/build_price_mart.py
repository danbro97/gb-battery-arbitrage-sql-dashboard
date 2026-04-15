import duckdb

con = duckdb.connect("db/gb_battery.duckdb")

con.execute("""
DROP TABLE IF EXISTS marts.power_prices_half_hourly_2025;
""")

con.execute("""
CREATE TABLE marts.power_prices_half_hourly_2025 AS
SELECT
    start_time,
    settlement_date,
    settlement_period,
    price_gbp_per_mwh,
    volume_mwh,
    data_provider,

    EXTRACT(year FROM start_time) AS year_num,
    EXTRACT(month FROM start_time) AS month_num,
    STRFTIME(start_time, '%Y-%m') AS year_month,
    STRFTIME(start_time, '%A') AS day_name,
    CASE
        WHEN EXTRACT(ISODOW FROM start_time) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    EXTRACT(hour FROM start_time) AS hour_num,

    CASE
        WHEN EXTRACT(month FROM start_time) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(month FROM start_time) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(month FROM start_time) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season
FROM raw.mid_prices_2025;
""")

print(con.execute("""
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT settlement_date) AS day_count,
    MIN(start_time) AS min_time,
    MAX(start_time) AS max_time
FROM marts.power_prices_half_hourly_2025
""").fetchdf())

print(con.execute("""
SELECT year_month, COUNT(*) AS rows_in_month
FROM marts.power_prices_half_hourly_2025
GROUP BY 1
ORDER BY 1
LIMIT 5
""").fetchdf())

con.close()