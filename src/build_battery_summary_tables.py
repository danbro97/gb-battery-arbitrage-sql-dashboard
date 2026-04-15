import duckdb

con = duckdb.connect("db/gb_battery.duckdb")

con.execute("""
DROP TABLE IF EXISTS marts.battery_dispatch_scenarios_enriched;
""")

con.execute("""
CREATE TABLE marts.battery_dispatch_scenarios_enriched AS
SELECT
    s.settlement_date,
    s.battery_power_mw,
    s.battery_duration_h,
    s.battery_energy_mwh,
    s.round_trip_efficiency,
    s.charge_block_start,
    s.discharge_block_start,
    s.avg_charge_price_gbp_per_mwh,
    s.avg_discharge_price_gbp_per_mwh,
    s.charge_cost_gbp,
    s.discharge_revenue_gbp,
    s.gross_revenue_gbp,
    s.profitable_flag,
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
""")

con.execute("""
DROP TABLE IF EXISTS marts.battery_scenario_summary_annual;
""")

con.execute("""
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
""")

con.execute("""
DROP TABLE IF EXISTS marts.battery_scenario_summary_monthly;
""")

con.execute("""
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
""")

con.execute("""
DROP TABLE IF EXISTS marts.battery_scenario_summary_day_type;
""")

con.execute("""
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
""")

print(con.execute("""
SELECT * 
FROM marts.battery_scenario_summary_annual
ORDER BY battery_duration_h, round_trip_efficiency
""").fetchdf())

print(con.execute("""
SELECT *
FROM marts.battery_scenario_summary_monthly
WHERE battery_duration_h = 2.0
  AND round_trip_efficiency = 0.90
ORDER BY month_num
LIMIT 12
""").fetchdf())

con.close()