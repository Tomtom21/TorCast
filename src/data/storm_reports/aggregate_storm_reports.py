"""
Aggregates storm report data from multiple CSV files into a single Parquet file.
"""

import os
from pathlib import Path
import sys
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from constants import storm_report_headers, common_storm_report_headers, geo_bounds

# Get the data directories by navigating ../data from the current working directory
data_dir = (Path.cwd() / "../../data").resolve()
hail_storm_reports_dir = data_dir / "raw" / "storm_report_data" / "hail"
tornado_storm_reports_dir = data_dir / "raw" / "storm_report_data" / "tornado"
wind_storm_reports_dir = data_dir / "raw" / "storm_report_data" / "wind"

# Save directory for the aggregated Parquet file
aggregated_output_dir = data_dir / "processed" / "storm_report_data"
aggregated_output_dir.mkdir(parents=True, exist_ok=True)
aggregated_output_file = aggregated_output_dir / "storm_report_data.parquet"

# Get the contents of each directory and combine all file paths into one list
hail_files = [hail_storm_reports_dir / f for f in os.listdir(hail_storm_reports_dir)]
tornado_files = [tornado_storm_reports_dir / f for f in os.listdir(tornado_storm_reports_dir)]
wind_files = [wind_storm_reports_dir / f for f in os.listdir(wind_storm_reports_dir)]

# Combining and sorting all report files paths into a single list
all_files = hail_files + tornado_files + wind_files
all_files.sort()

# Collect all DataFrames in a list that we concat later
df_list = []

# Margin for geo bounds
GEO_MARGIN = 10.0

# Loop through all_files with tqdm and add each report to the list
for file_path in tqdm(all_files, desc="Aggregating storm reports"):
    # Determine the type based on the file path
    if "hail" in str(file_path).lower():
        report_type = "hail"
    elif "tornado" in str(file_path).lower():
        report_type = "torn"
    elif "wind" in str(file_path).lower():
        report_type = "wind"
    else:
        continue  # skip unknown types

    header = storm_report_headers[report_type]

    # Read the CSV file
    df = pd.read_csv(file_path, usecols=range(len(header)))

    # Loading the file again if the dataframe we have doesn't have headers
    if list(df.columns) != header:
        df = pd.read_csv(
            file_path,
            names=header,
            header=0 if len(df.columns) == len(header) else None,
            usecols=range(len(header))
        )

    # Extract date from the filename by splitting on underscore
    stem = file_path.stem  # e.g., '110520_rpts_hail'
    parts = stem.split('_')
    date_part = parts[0] if parts else stem

    if not len(date_part) == 6 or not date_part.isdigit():
        print(f"Unexpected date format in filename: {file_path.name}")
        continue

    yy = date_part[:2]
    mm = date_part[2:4]
    dd = date_part[4:6]
    formatted_date = f"20{yy}-{mm}-{dd}"

    df["Date"] = formatted_date
    df["Type"] = report_type

    # Remove rows where State is not exactly 2 characters
    df = df[df["State"].astype(str).str.len() == 2]

    # Convert Lat/Lon to float, coerce errors to NaN
    df["Lat"] = pd.to_numeric(df["Lat"], errors="coerce")
    df["Lon"] = pd.to_numeric(df["Lon"], errors="coerce")

    # Remove rows with Lat/Lon outside bounds (+/- margin)
    lat_min = geo_bounds["us_lat_min"] - GEO_MARGIN
    lat_max = geo_bounds["us_lat_max"] + GEO_MARGIN
    lon_min = geo_bounds["us_lon_min"] - GEO_MARGIN
    lon_max = geo_bounds["us_lon_max"] + GEO_MARGIN

    df = df[
        df["Lat"].between(lat_min, lat_max) &
        df["Lon"].between(lon_min, lon_max)
    ]

    # Create UTC_Timestamp column by combining Date and Time
    def make_utc(row):
        """
        Creates a UTC timestamp from the date and time columns.
        """
        # Time is in military format, e.g., '2130'
        time_str = str(row["Time"]).zfill(4)
        date_str = row["Date"]
        try:
            dt = datetime.strptime(date_str + time_str, "%Y-%m-%d%H%M")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    df["UTC_Timestamp"] = df.apply(make_utc, axis=1)

    # Selecting only the columns we want to keep
    df_common = df[common_storm_report_headers]

    # Append to the list only if df_common is not empty
    if not df_common.empty:
        df_list.append(df_common)

# Concatenate all DataFrames at once (avoids FutureWarning)
if df_list:
    main_df = pd.concat(df_list, ignore_index=True)
else:
    print("No valid storm report data to add. df_list is empty.")
    sys.exit(1)

# Sorting and saving the final dataframe to Parquet
main_df.sort_values(by="UTC_Timestamp", inplace=True, ascending=False)
main_df.to_parquet(aggregated_output_file, index=False)
print(f"Aggregated storm report data saved to {aggregated_output_file}. Saved {len(main_df)} records.")
