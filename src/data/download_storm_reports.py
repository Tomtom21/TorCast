import requests
from datetime import datetime, timedelta
import os
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import time

# Get the data directories by navigating ../../data from the current file's directory
data_dir = (Path(__file__).parent / "../../data").resolve()
storm_report_dir = data_dir / "raw" / "storm_report_data"
tornado_report_dir = storm_report_dir / "tornado"
hail_report_dir = storm_report_dir / "hail"
wind_report_dir = storm_report_dir / "wind"

# Ensure directories exist
tornado_report_dir.mkdir(parents=True, exist_ok=True)
hail_report_dir.mkdir(parents=True, exist_ok=True)
wind_report_dir.mkdir(parents=True, exist_ok=True)

def download_file_from_url(url, save_path, retries=3, delay=15, backoff_factor=2):
    """
    Download a file from the given URL and save it to the specified path.
    
    :param url: URL of the file to download
    :param save_path: Local path to save the downloaded file
    :return: (True, None) if download was successful, (False, error_message) otherwise
    """
    wait = delay
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()  # Raise an error for HTTP errors
            with open(save_path, 'wb') as f:
                f.write(resp.content)
            return True, None
        except Exception as e:
            if attempt == retries:
                return False, str(e)
            print(f"Failed. Sleeping for {wait} seconds before retrying...")
            time.sleep(wait)
            wait *= backoff_factor

def get_existing_report_types(date_str):
    """
    Return a set of report types ('torn', 'hail', 'wind') that exist for the given date.

    :param date_str: Date string in 'YYYYMMDD' format
    :return: set of report types that exist, or None if error
    """
    report_url = f"https://www.spc.noaa.gov/climo/reports/{date_str}_rpts.html"
    section_headers = {
        "torn": "Tornado Reports",
        "hail": "Hail Reports",
        "wind": "Wind Reports"
    }
    try:
        resp = requests.get(report_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        found = set()
        # Find all <th> tags that are section headers
        ths = soup.find_all("th", colspan="8")
        for th in ths:
            text = th.get_text(strip=True)
            for key, section in section_headers.items():
                if text.startswith(section):
                    # Check the next sibling row for "No reports received"
                    tr = th.find_parent("tr")
                    next_tr = tr.find_next_sibling("tr")
                    if next_tr:
                        td = next_tr.find("td", class_="highlight")
                        if td and "no reports received" in td.get_text(strip=True).lower():
                            continue
                    found.add(key)
        return found
    except Exception:
        return None

# Generating the list of date strings, from 2004-04-01 to one week ago
start_date = datetime(2004, 4, 1)
end_date = datetime.today() - timedelta(weeks=1)

date_strings = []
current_date = start_date
while current_date <= end_date:
    date_strings.append(current_date.strftime('%y%m%d'))
    current_date += timedelta(days=1)

# Downloading each of the reports
for ds in tqdm(date_strings, desc="Processing Dates"):
    existing_types = get_existing_report_types(ds)
    if existing_types is None:
        print(f"Could not determine report types for {ds}")
        continue

    report_info = [
        ("torn", tornado_report_dir, f'{ds}_rpts_torn.csv'),
        ("hail", hail_report_dir, f'{ds}_rpts_hail.csv'),
        ("wind", wind_report_dir, f'{ds}_rpts_wind.csv'),
    ]

    for report_type, report_dir, filename in report_info:
        save_path = report_dir / filename
        if report_type in existing_types:
            base_url = f'https://www.spc.noaa.gov/climo/reports/{ds}_rpts_{report_type}.csv'
            success, error = download_file_from_url(base_url, save_path)
            if success:
                print(f"Downloaded {report_type} report for {ds}")
            else:
                print(f"Failed to download {report_type} report for {ds}: {error}")
        else:
            # Generate placeholder CSV file with only the header
            headers = {
                "torn": ["Time", "F_Scale", "Location", "County", "State", "Lat", "Lon", "Comments"],
                "hail": ["Time", "Size", "Location", "County", "State", "Lat", "Lon", "Comments"],
                "wind": ["Time", "Speed", "Location", "County", "State", "Lat", "Lon", "Comments"]
            }
            pd.DataFrame(columns=headers[report_type]).to_csv(save_path, index=False)
            print(f"Generated placeholder {report_type} report for {ds}")

