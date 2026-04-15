import duckdb

con = duckdb.connect("db/gb_battery.duckdb")

con.execute("""
DROP TABLE IF EXISTS marts.daily_price_shape_features_2025;
""")

con.execute("""
CREATE TABLE marts.daily_price_shape_features_2025 AS
WITH daily_base AS (
    SELECT
        settlement_date,
        MIN(year_num) AS year_num,
        MIN(month_num) AS month_num,
        MIN(year_month) AS year_month,
        MIN(day_name) AS day_name,
        MIN(day_type) AS day_type,
        MIN(season) AS season,
        COUNT(*) AS half_hour_count,
        AVG(price_gbp_per_mwh) AS avg_price,
        MIN(price_gbp_per_mwh) AS min_price,
        MAX(price_gbp_per_mwh) AS max_price,
        MAX(price_gbp_per_mwh) - MIN(price_gbp_per_mwh) AS intraday_spread,
        SUM(CASE WHEN price_gbp_per_mwh < 0 THEN 1 ELSE 0 END) AS negative_price_periods
    FROM marts.power_prices_half_hourly_2025
    GROUP BY settlement_date
),
ranked_prices AS (
    SELECT
        settlement_date,
        price_gbp_per_mwh,
        ROW_NUMBER() OVER (
            PARTITION BY settlement_date
            ORDER BY price_gbp_per_mwh ASC, start_time ASC
        ) AS low_rank,
        ROW_NUMBER() OVER (
            PARTITION BY settlement_date
            ORDER BY price_gbp_per_mwh DESC, start_time ASC
        ) AS high_rank
    FROM marts.power_prices_half_hourly_2025
),
top_bottom_avgs AS (
    SELECT
        settlement_date,
        AVG(CASE WHEN low_rank <= 4 THEN price_gbp_per_mwh END) AS avg_2h_low_price,
        AVG(CASE WHEN high_rank <= 4 THEN price_gbp_per_mwh END) AS avg_2h_high_price
    FROM ranked_prices
    GROUP BY settlement_date
)
SELECT
    b.*,
    t.avg_2h_low_price,
    t.avg_2h_high_price,
    t.avg_2h_high_price - t.avg_2h_low_price AS top_bottom_2h_spread
FROM daily_base b
LEFT JOIN top_bottom_avgs t
    ON b.settlement_date = t.settlement_date
ORDER BY b.settlement_date;
""")

print(con.execute("""
SELECT
    COUNT(*) AS day_count,
    MIN(settlement_date) AS min_date,
    MAX(settlement_date) AS max_date,
    MIN(intraday_spread) AS min_intraday_spread,
    MAX(intraday_spread) AS max_intraday_spread
FROM marts.daily_price_shape_features_2025
""").fetchdf())

print(con.execute("""
SELECT
    settlement_date,
    day_type,
    season,
    avg_price,
    min_price,
    max_price,
    intraday_spread,
    top_bottom_2h_spread,
    negative_price_periods
FROM marts.daily_price_shape_features_2025
ORDER BY intraday_spread DESC
LIMIT 10
""").fetchdf())

con.close()