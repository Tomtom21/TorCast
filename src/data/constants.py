storm_report_headers = {
    "torn": ["Time", "F_Scale", "Location", "County", "State", "Lat", "Lon", "Comments"],
    "hail": ["Time", "Size", "Location", "County", "State", "Lat", "Lon", "Comments"],
    "wind": ["Time", "Speed", "Location", "County", "State", "Lat", "Lon", "Comments"]
}

common_storm_report_headers = [
    "Date", 
    "Time", 
    "Type",
    "Location", 
    "County", 
    "State", 
    "Lat", 
    "Lon"
]

geo_bounds = {
    "us_lat_min": 24.0,
    "us_lat_max": 50.0,
    "us_lon_min": -126.0,
    "us_lon_max": -65.5
}
