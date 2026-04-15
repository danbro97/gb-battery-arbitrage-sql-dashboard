from pathlib import Path
import duckdb

DB_PATH = "db/gb_battery.duckdb"

def run_sql_file(con, path):
    sql = Path(path).read_text(encoding="utf-8")
    con.execute(sql)

con = duckdb.connect(DB_PATH)

run_sql_file(con, "sql/04_build_battery_summary_tables.sql")

print(con.execute("""
SELECT *
FROM marts.battery_scenario_summary_annual
ORDER BY battery_duration_h, round_trip_efficiency
""").fetchdf())

con.close()