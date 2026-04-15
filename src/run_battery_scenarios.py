import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect("db/gb_battery.duckdb")

prices = con.execute("""
    SELECT
        start_time,
        settlement_date,
        settlement_period,
        price_gbp_per_mwh
    FROM marts.power_prices_half_hourly_2025
    ORDER BY settlement_date, start_time
""").fetchdf()

battery_power_mw = 1.0
durations_h = [1.0, 2.0, 4.0]
round_trip_efficiencies = [0.85, 0.90, 0.925]

results = []

daily_groups = list(prices.groupby("settlement_date"))
total_scenarios = len(durations_h) * len(round_trip_efficiencies)

scenario_counter = 0

for battery_duration_h in durations_h:
    battery_energy_mwh = battery_power_mw * battery_duration_h
    block_length = int(battery_duration_h * 2)

    for round_trip_efficiency in round_trip_efficiencies:
        scenario_counter += 1
        print(
            f"Running scenario {scenario_counter}/{total_scenarios}: "
            f"duration={battery_duration_h}h, rte={round_trip_efficiency}"
        )

        charge_efficiency = round_trip_efficiency ** 0.5
        discharge_efficiency = round_trip_efficiency ** 0.5

        charge_energy_from_grid_mwh = battery_energy_mwh / charge_efficiency
        discharge_energy_to_grid_mwh = battery_energy_mwh * discharge_efficiency

        for day_idx, (settlement_date, day_df) in enumerate(daily_groups, start=1):
            if day_idx % 50 == 0:
                print(f"  Processed {day_idx}/{len(daily_groups)} days")

            day_df = day_df.sort_values("start_time").reset_index(drop=True).copy()
            prices_arr = day_df["price_gbp_per_mwh"].to_numpy()
            times_arr = day_df["start_time"].to_numpy()

            n = len(prices_arr)
            if n < 2 * block_length:
                results.append(
                    {
                        "settlement_date": settlement_date,
                        "battery_power_mw": battery_power_mw,
                        "battery_duration_h": battery_duration_h,
                        "battery_energy_mwh": battery_energy_mwh,
                        "round_trip_efficiency": round_trip_efficiency,
                        "charge_block_start": None,
                        "discharge_block_start": None,
                        "avg_charge_price_gbp_per_mwh": None,
                        "avg_discharge_price_gbp_per_mwh": None,
                        "charge_cost_gbp": 0.0,
                        "discharge_revenue_gbp": 0.0,
                        "gross_revenue_gbp": 0.0,
                        "profitable_flag": False,
                    }
                )
                continue

            # Rolling block means
            block_means = np.array([
                prices_arr[i:i + block_length].mean()
                for i in range(n - block_length + 1)
            ])

            best_gross_revenue = -np.inf
            best_charge_start = None
            best_discharge_start = None
            best_avg_charge_price = None
            best_avg_discharge_price = None

            for charge_start in range(0, len(block_means)):
                first_valid_discharge = charge_start + block_length
                if first_valid_discharge >= len(block_means):
                    continue

                discharge_slice = block_means[first_valid_discharge:]
                rel_best_discharge_idx = int(np.argmax(discharge_slice))
                discharge_start = first_valid_discharge + rel_best_discharge_idx

                avg_charge_price = block_means[charge_start]
                avg_discharge_price = block_means[discharge_start]

                charge_cost_gbp = avg_charge_price * charge_energy_from_grid_mwh
                discharge_revenue_gbp = avg_discharge_price * discharge_energy_to_grid_mwh
                gross_revenue_gbp = discharge_revenue_gbp - charge_cost_gbp

                if gross_revenue_gbp > best_gross_revenue:
                    best_gross_revenue = gross_revenue_gbp
                    best_charge_start = charge_start
                    best_discharge_start = discharge_start
                    best_avg_charge_price = avg_charge_price
                    best_avg_discharge_price = avg_discharge_price

            if best_gross_revenue > 0:
                charge_cost_gbp = best_avg_charge_price * charge_energy_from_grid_mwh
                discharge_revenue_gbp = best_avg_discharge_price * discharge_energy_to_grid_mwh

                results.append(
                    {
                        "settlement_date": settlement_date,
                        "battery_power_mw": battery_power_mw,
                        "battery_duration_h": battery_duration_h,
                        "battery_energy_mwh": battery_energy_mwh,
                        "round_trip_efficiency": round_trip_efficiency,
                        "charge_block_start": pd.Timestamp(times_arr[best_charge_start]),
                        "discharge_block_start": pd.Timestamp(times_arr[best_discharge_start]),
                        "avg_charge_price_gbp_per_mwh": best_avg_charge_price,
                        "avg_discharge_price_gbp_per_mwh": best_avg_discharge_price,
                        "charge_cost_gbp": charge_cost_gbp,
                        "discharge_revenue_gbp": discharge_revenue_gbp,
                        "gross_revenue_gbp": best_gross_revenue,
                        "profitable_flag": True,
                    }
                )
            else:
                results.append(
                    {
                        "settlement_date": settlement_date,
                        "battery_power_mw": battery_power_mw,
                        "battery_duration_h": battery_duration_h,
                        "battery_energy_mwh": battery_energy_mwh,
                        "round_trip_efficiency": round_trip_efficiency,
                        "charge_block_start": None,
                        "discharge_block_start": None,
                        "avg_charge_price_gbp_per_mwh": None,
                        "avg_discharge_price_gbp_per_mwh": None,
                        "charge_cost_gbp": 0.0,
                        "discharge_revenue_gbp": 0.0,
                        "gross_revenue_gbp": 0.0,
                        "profitable_flag": False,
                    }
                )

results_df = pd.DataFrame(results)

con.execute("DROP TABLE IF EXISTS marts.battery_dispatch_scenarios_daily")
con.register("results_df", results_df)

con.execute("""
    CREATE TABLE marts.battery_dispatch_scenarios_daily AS
    SELECT * FROM results_df
""")

print(con.execute("""
    SELECT
        battery_duration_h,
        round_trip_efficiency,
        COUNT(*) AS day_count,
        SUM(gross_revenue_gbp) AS annual_gross_revenue_gbp,
        AVG(gross_revenue_gbp) AS avg_daily_gross_revenue_gbp,
        SUM(CASE WHEN profitable_flag THEN 1 ELSE 0 END) AS profitable_days
    FROM marts.battery_dispatch_scenarios_daily
    GROUP BY 1, 2
    ORDER BY 1, 2
""").fetchdf())

con.close()