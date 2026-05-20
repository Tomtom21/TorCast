from datetime import datetime

import pandas as pd

from ..constants import storm_report_headers, common_storm_report_headers, geo_bounds, GEO_MARGIN

class StormReport:
    def __init__(self, file_path):
        self.file_path = file_path

        # Determining the report type based on the file name or path
        self.report_type = self._extract_report_type()

        # Reading the CSV file
        header = storm_report_headers[self.report_type]
        self.df = pd.read_csv(file_path, usecols=range(len(header)))

        # Extracting/setting the date and report type
        self.df["Date"] = self._extract_date()
        self.df["Type"] = self.report_type

        # Removing rows with invalid values
        self._clean_invalid_rows()

        # Adding UTC timestamp column
        self._military_to_utc()


    def _extract_date(self):
        """
        Extracts the date from the filename

        :raises ValueError: if the filename doesn't match the expected format
        """
        stem = self.file_path.stem  # e.g., '110520_rpts_hail'
        parts = stem.split("_")
        date_part = parts[0] if parts else stem

        if not len(date_part) == 6 or not date_part.isdigit():
            raise ValueError(f"Unexpected date format in filename: {self.file_path.name}")
        yy = date_part[:2]
        mm = date_part[2:4]
        dd = date_part[4:6]
        return f"20{yy}-{mm}-{dd}"
    
    def _extract_report_type(self):
        """
        Extracts the report type from the filename

        :raises ValueError: if the filename doesn't contain a recognizable report type
        """
        file_path_str = str(self.file_path).lower()
        if "hail" in file_path_str:
            return "hail"
        elif "tornado" in file_path_str:
            return "torn"
        elif "wind" in file_path_str:
            return "wind"
        else:
            raise ValueError(f"Unknown report type for file: {self.file_path}")
        
    def _clean_invalid_rows(self):
        """
        Removes rows with invalid Lat/Lon values or where State is not exactly 2 characters
        """
        # Remove rows where State is not exactly 2 characters
        self.df = self.df[self.df["State"].astype(str).str.len() == 2]

        # Convert Lat/Lon to float, coerce errors to NaN
        self.df["Lat"] = pd.to_numeric(self.df["Lat"], errors="coerce")
        self.df["Lon"] = pd.to_numeric(self.df["Lon"], errors="coerce")

        # Remove rows with Lat/Lon outside bounds (+/- margin)
        lat_min = geo_bounds["us_lat_min"] - GEO_MARGIN
        lat_max = geo_bounds["us_lat_max"] + GEO_MARGIN
        lon_min = geo_bounds["us_lon_min"] - GEO_MARGIN
        lon_max = geo_bounds["us_lon_max"] + GEO_MARGIN

        self.df = self.df[
            self.df["Lat"].between(lat_min, lat_max) &
            self.df["Lon"].between(lon_min, lon_max)
        ]

    def _military_to_utc(self):
        """
        Converts the date and time from the report into a UTC timestamp

        :return: The UTC timestamp in ISO 8601 format
        """
        self.df["Time"] = self.df["Time"].astype(str).str.zfill(4)
        utc_timestamps = pd.to_datetime(
            self.df["Date"] + self.df["Time"], 
            format="%Y-%m-%d%H%M", 
            errors="coerce"
        )
        self.df["UTC_Timestamp"] = utc_timestamps.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
