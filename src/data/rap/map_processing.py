from constants import geo_bounds
import numpy as np

class NARRDataProcessing:
    @staticmethod
    def _get_xy_bounds_for_us(narr_data):
        # Extracting the lat/lon arrays
        lats = narr_data.lat
        lons = narr_data.lon

        # Creating a mask for the region that we care about
        mask = (
            (lats >= geo_bounds["us_lat_min"]) & (lats <= geo_bounds["us_lat_max"]) &
            (lons >= geo_bounds["us_lon_min"]) & (lons <= geo_bounds["us_lon_max"])
        )

        # Find the indices where that fall inside the region
        valid_y, valid_x = np.where(mask)

        # Use those to find the corresponding x/y limits
        x_coords = narr_data.x.values
        y_coords = narr_data.y.values

        x_min = x_coords[valid_x.min()]
        x_max = x_coords[valid_x.max()]
        y_min = y_coords[valid_y.min()]
        y_max = y_coords[valid_y.max()]

        return x_min, x_max, y_min, y_max

    @staticmethod
    def crop_to_us_bounds(narr_data):
        x_min, x_max, y_min, y_max = NARRDataProcessing._get_xy_bounds_for_us(narr_data)

        cropped_data = narr_data.sel(
            x=slice(x_min, x_max),
            y=slice(y_min, y_max)
        )

        return cropped_data
