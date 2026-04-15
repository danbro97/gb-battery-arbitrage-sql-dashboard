from pathlib import Path
import json
import pandas as pd

RAW_DIR = Path("data_raw/prices")
OUT_DIR = Path("data_processed/analytical")
OUT_DIR.mkdir(parents=True, exist_ok=True)

json_files = sorted(RAW_DIR.glob("mid_*.json"))

frames = []

for path in json_files:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("data", [])
    if not rows:
        print(f"Skipping empty file: {path.name}")
        continue

    df = pd.DataFrame(rows)
    df["source_file"] = path.name
    frames.append(df)

if not frames:
    raise ValueError("No price data found.")

prices = pd.concat(frames, ignore_index=True)

prices["startTime"] = pd.to_datetime(prices["startTime"], utc=True)
prices["settlementDate"] = pd.to_datetime(prices["settlementDate"])

prices = prices.sort_values(["startTime", "dataProvider"]).reset_index(drop=True)

print("Providers summary:")
print(
    prices.groupby("dataProvider", dropna=False)
    .agg(
        rows=("price", "size"),
        non_zero_volume_rows=("volume", lambda s: (s > 0).sum()),
    )
    .reset_index()
)

# Keep APXMIDP only
prices = prices.loc[prices["dataProvider"] == "APXMIDP"].copy()

# Hard filter to 2025 only because API chunk boundaries can include the upper bound
prices = prices[
    (prices["startTime"] >= pd.Timestamp("2025-01-01 00:00:00+00:00"))
    & (prices["startTime"] < pd.Timestamp("2026-01-01 00:00:00+00:00"))
].copy()

prices = prices[
    [
        "startTime",
        "settlementDate",
        "settlementPeriod",
        "price",
        "volume",
        "dataProvider",
        "source_file",
    ]
].sort_values("startTime").reset_index(drop=True)

# Check duplicates before removing them
dupes = prices[prices.duplicated(subset=["startTime"], keep=False)].copy()
print(f"Duplicate timestamp rows before dedup: {len(dupes):,}")

if len(dupes) > 0:
    dupes = dupes.sort_values(["startTime", "source_file"])
    dupes.to_csv(OUT_DIR / "mid_2025_apxmidp_duplicate_timestamps.csv", index=False)
    print("Saved duplicate timestamps file.")
    print(dupes.head(20))

# Remove duplicate boundary rows
prices = prices.drop_duplicates(subset=["startTime"], keep="first").copy()
prices = prices.sort_values("startTime").reset_index(drop=True)

# Check completeness
expected = pd.date_range(
    start="2025-01-01 00:00:00+00:00",
    end="2025-12-31 23:30:00+00:00",
    freq="30min",
    tz="UTC",
)

actual = pd.DatetimeIndex(prices["startTime"].unique())
missing = expected.difference(actual)

print(f"APXMIDP final rows: {len(prices):,}")
print(f"Expected half-hours in 2025: {len(expected):,}")
print(f"Actual unique half-hours: {len(actual):,}")
print(f"Missing half-hours: {len(missing):,}")

if len(missing) > 0:
    missing_df = pd.DataFrame({"missing_startTime": missing})
    missing_df.to_csv(OUT_DIR / "mid_2025_missing_half_hours.csv", index=False)
    print("Saved missing half-hours file.")

prices.to_csv(OUT_DIR / "mid_2025_apxmidp.csv", index=False)

print(prices.head())
print(prices.columns.tolist())