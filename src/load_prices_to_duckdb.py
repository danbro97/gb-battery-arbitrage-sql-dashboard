from pathlib import Path
import duckdb

DB_PATH = "db/gb_battery.duckdb"

def run_sql_file(con, path):
    sql = Path(path).read_text(encoding="utf-8")
    con.execute(sql)

con = duckdb.connect(DB_PATH)

run_sql_file(con, "sql/01_create_schemas.sql")
run_sql_file(con, "sql/02_load_prices.sql")

print(con.execute("""
SELECT
    COUNT(*) AS row_count,
    MIN(start_time) AS min_time,
    MAX(start_time) AS max_time,
    MIN(price_gbp_per_mwh) AS min_price,
    MAX(price_gbp_per_mwh) AS max_price
FROM raw.mid_prices_2025
""").fetchdf())

con.close()