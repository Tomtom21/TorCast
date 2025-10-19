"""
Aggregates storm report data from multiple CSV files into a single Parquet file.
"""

import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from constants import storm_report_headers, common_storm_report_headers


# Get the data directories by navigating ../data from the current working directory
data_dir = (Path.cwd() / "../../data").resolve()
hail_storm_reports_dir = data_dir / "raw" / "storm_report_data" / "hail"
tornado_storm_reports_dir = data_dir / "raw" / "storm_report_data" / "tornado"
wind_storm_reports_dir = data_dir / "raw" / "storm_report_data" / "wind"

# Get the contents of each directory and combine all file paths into one list
hail_files = [hail_storm_reports_dir / f for f in os.listdir(hail_storm_reports_dir)]
tornado_files = [tornado_storm_reports_dir / f for f in os.listdir(tornado_storm_reports_dir)]
wind_files = [wind_storm_reports_dir / f for f in os.listdir(wind_storm_reports_dir)]

# Combining and sorting all report files paths into a single list
all_files = hail_files + tornado_files + wind_files
all_files.sort()

# Collect all DataFrames in a list that we concat later
df_list = []

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

    mm = date_part[:2]
    dd = date_part[2:4]
    yy = date_part[4:6]
    formatted_date = f"20{yy}-{mm}-{dd}"

    df["Date"] = formatted_date

    # Add the Type column
    df["Type"] = report_type

    # Reorder and select only the common columns
    df_common = df[common_storm_report_headers]
    
    # Append to the list only if df_common is not empty
    if not df_common.empty:
        df_list.append(df_common)

# Concatenate all DataFrames at once (avoids FutureWarning)
if df_list:
    main_df = pd.concat(df_list, ignore_index=True)
else:
    main_df = pd.DataFrame(columns=common_storm_report_headers)

print(len(main_df))
print(main_df.head(50))
print(main_df[(main_df["Type"] == "torn")].head(50))
