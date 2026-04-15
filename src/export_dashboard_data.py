import duckdb
from pathlib import Path

OUT_DIR = Path("data_processed/exports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect("db/gb_battery.duckdb")

exports = {
    "battery_scenario_summary_annual.csv": """
        SELECT *
        FROM marts.battery_scenario_summary_annual
        ORDER BY battery_duration_h, round_trip_efficiency
    """,
    "battery_scenario_summary_monthly.csv": """
        SELECT *
        FROM marts.battery_scenario_summary_monthly
        ORDER BY battery_duration_h, round_trip_efficiency, month_num
    """,
    "battery_scenario_summary_day_type.csv": """
        SELECT *
        FROM marts.battery_scenario_summary_day_type
        ORDER BY battery_duration_h, round_trip_efficiency, day_type, season
    """,
    "battery_dispatch_scenarios_enriched.csv": """
        SELECT *
        FROM marts.battery_dispatch_scenarios_enriched
        ORDER BY battery_duration_h, round_trip_efficiency, settlement_date
    """
}

for filename, query in exports.items():
    out_path = OUT_DIR / filename
    con.execute(f"""
        COPY ({query})
        TO '{out_path.as_posix()}'
        WITH (HEADER, DELIMITER ',');
    """)
    print(f"Exported: {out_path}")

con.close()
print("Done.")