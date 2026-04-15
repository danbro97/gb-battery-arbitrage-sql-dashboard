from pathlib import Path
import duckdb

DB_PATH = "db/gb_battery.duckdb"

def run_sql_file(con, path):
    sql = Path(path).read_text(encoding="utf-8")
    con.execute(sql)

con = duckdb.connect(DB_PATH)

run_sql_file(con, "sql/03_build_price_mart.sql")

print(con.execute("""
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT settlement_date) AS day_count,
    MIN(start_time) AS min_time,
    MAX(start_time) AS max_time
FROM marts.power_prices_half_hourly_2025
""").fetchdf())

con.close()