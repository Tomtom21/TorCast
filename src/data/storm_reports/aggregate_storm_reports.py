"""
Aggregates storm report data from multiple CSV files into a single Parquet file.
"""

import os
from pathlib import Path
import sys
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from src.data.constants import storm_report_headers, common_storm_report_headers, geo_bounds
from src.data.storm_reports.storm_report import StormReport

# Get the data directories by navigating ../../data from the current working directory
data_dir = (Path.cwd() / "../../../data").resolve()
hail_storm_reports_dir = data_dir / "raw" / "storm_reports" / "hail"
tornado_storm_reports_dir = data_dir / "raw" / "storm_reports" / "tornado"
wind_storm_reports_dir = data_dir / "raw" / "storm_reports" / "wind"

# Save directory for the aggregated Parquet file
aggregated_output_dir = data_dir / "processed" / "storm_reports"
aggregated_output_dir.mkdir(parents=True, exist_ok=True)
aggregated_output_file = aggregated_output_dir / "storm_reports.parquet"

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
    # Parsing the storm report
    report = StormReport(file_path)
    
    # Selecting only the columns we want to keep
    df_common = report.df[common_storm_report_headers]

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
