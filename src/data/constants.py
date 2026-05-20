# The expected headers on the respective storm reports
storm_report_headers = {
    "torn": ["Time", "F_Scale", "Location", "County", "State", "Lat", "Lon", "Comments"],
    "hail": ["Time", "Size", "Location", "County", "State", "Lat", "Lon", "Comments"],
    "wind": ["Time", "Speed", "Location", "County", "State", "Lat", "Lon", "Comments"]
}

# Anticipated headers on a storm report that we care about
common_storm_report_headers = [
    "UTC_Timestamp",
    "Type",
    "Lat", 
    "Lon"
]

# The geographical boundaries in lat/long for the input maps
geo_bounds = {
    "us_lat_min": 24.0,
    "us_lat_max": 50.0,
    "us_lon_min": -126.0,
    "us_lon_max": -65.5
}

# Margin to apply to the lat/lon bounds when filtering storm reports
GEO_MARGIN = 10.0
