This is where all raw and processed data goes.

The data directory is structured as follows:

Example layout:
```
data
├── readme.md
├── raw/
│   ├── storm_report_data/
│   │   ├── hail/
│   │   │   ├── hail_2020.csv
│   │   │   ├── hail_2021.csv
│   │   │   └── ...
│   │   ├── tornado/
│   │   │   ├── tornado_2020.csv
│   │   │   ├── tornado_2021.csv
│   │   │   └── ...
│   │   └── wind/
│   │       ├── wind_2020.csv
│   │       ├── wind_2021.csv
│   │       └── ...
│   └── weather_data/
│       ├── cape/
│       │   ├── cape_2020.csv
│       │   ├── cape_2021.csv
│       │   └── ...
│       ├── temperature/
│       │   ├── temperature_2020.csv
│       │   ├── temperature_2021.csv
│       │   └── ...
│       ├── dew_point/
│       │   ├── dew_point_2020.csv
│       │   ├── dew_point_2021.csv
│       │   └── ...
│       └── ... (other weather variables)
└── processed/
    ├── data_2020.h5
    ├── data_2021.h5
    └── ... (other processed files)
```
- All raw, unprocessed data is stored in the `raw/` directory.
- Weather data is organized by variable in subdirectories under `weather_data/`.
- All processed data files (e.g., `.h5` files) are stored in the `processed/` directory.


