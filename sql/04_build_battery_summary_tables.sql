DROP TABLE IF EXISTS marts.battery_dispatch_scenarios_enriched;

CREATE TABLE marts.battery_dispatch_scenarios_enriched AS
SELECT
    s.*,
    d.year_num,
    d.month_num,
    d.year_month,
    d.day_name,
    d.day_type,
    d.season,
    d.avg_price,
    d.min_price,
    d.max_price,
    d.intraday_spread,
    d.top_bottom_2h_spread,
    d.negative_price_periods
FROM marts.battery_dispatch_scenarios_daily s
LEFT JOIN marts.daily_price_shape_features_2025 d
    ON s.settlement_date = d.settlement_date;

DROP TABLE IF EXISTS marts.battery_scenario_summary_annual;

CREATE TABLE marts.battery_scenario_summary_annual AS
SELECT
    battery_duration_h,
    round_trip_efficiency,
    COUNT(*) AS day_count,
    SUM(gross_revenue_gbp) AS annual_gross_revenue_gbp,
    AVG(gross_revenue_gbp) AS avg_daily_gross_revenue_gbp,
    SUM(CASE WHEN profitable_flag THEN 1 ELSE 0 END) AS profitable_days
FROM marts.battery_dispatch_scenarios_enriched
GROUP BY 1, 2
ORDER BY 1, 2;

DROP TABLE IF EXISTS marts.battery_scenario_summary_monthly;

CREATE TABLE marts.battery_scenario_summary_monthly AS
SELECT
    battery_duration_h,
    round_trip_efficiency,
    year_month,
    month_num,
    SUM(gross_revenue_gbp) AS monthly_gross_revenue_gbp,
    AVG(gross_revenue_gbp) AS avg_daily_gross_revenue_gbp,
    SUM(CASE WHEN profitable_flag THEN 1 ELSE 0 END) AS profitable_days
FROM marts.battery_dispatch_scenarios_enriched
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 4;

DROP TABLE IF EXISTS marts.battery_scenario_summary_day_type;

CREATE TABLE marts.battery_scenario_summary_day_type AS
SELECT
    battery_duration_h,
    round_trip_efficiency,
    day_type,
    season,
    SUM(gross_revenue_gbp) AS gross_revenue_gbp,
    AVG(gross_revenue_gbp) AS avg_daily_gross_revenue_gbp,
    COUNT(*) AS day_count
FROM marts.battery_dispatch_scenarios_enriched
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;