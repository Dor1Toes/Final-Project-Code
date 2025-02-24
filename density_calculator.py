import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os
from datetime import datetime

# Define radius to compute density
RADIUS = 1000

# Define POI categories
POI_CATEGORY_DICT = {
    "transport": {"stop_position", "bus_station", "bus_stop", "tram_stop", "halt", "station", "taxi"},
    "shopping": {"supermarket", "mall", "department_store"},
    "education": {"school", "university", "college", "library"},
    "healthcare": {"hospital", "clinic", "veterinary", "pharmacy"},
}

# Global rating average & review threshold
MEAN = 3.5  # Assume average rating is 3.5
MIN_REVIEWS = 50  # Minimum reviews to consider rating reliable


class DensityCalculator:
    def __init__(self, poi_path, radius=RADIUS, categories_dict=POI_CATEGORY_DICT):
        self.radius = radius
        self.poi_path = poi_path
        self.categories_dict = categories_dict


    def load_data_GeoDataFrame(self, file_path, row_lon, row_lat):
        """Loads data from CSV and converts it to a GeoDataFrame."""
        data_df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='replace')

        # Convert to GeoDataFrame
        data_df["geometry"] = data_df.apply(lambda row: Point(row[row_lon], row[row_lat]), axis=1)
        return gpd.GeoDataFrame(data_df, geometry="geometry", crs="EPSG:4326")


    def calculate_poi_density(self, business, pois_gdf, category_key=None):
        """Calculates the number of POIs within a given radius based on a category key."""
        buffer = business.geometry.buffer(self.radius)

        if category_key:
            valid_types = self.categories_dict.get(category_key, set())
            pois_gdf = pois_gdf[pois_gdf["type"].isin(valid_types)]

        return pois_gdf[pois_gdf.geometry.intersects(buffer)].shape[0]


    def calculate_competitor_density(self, business, businesses_gdf):
        """Calculates the number of competitors around a given business."""
        business_categories = set(str(business["categories"]).split(", "))

        competitors = businesses_gdf[businesses_gdf["categories"].apply(
            lambda x: bool(set(str(x).split(", ")).intersection(business_categories))
        )]

        competitors = competitors[competitors["business_id"] != business["business_id"]]

        buffer = business.geometry.buffer(self.radius)
        return competitors[competitors.geometry.intersects(buffer)].shape[0]


    def calculate_success_index(self, row, mean_stars =  MEAN, reviews_threshold = MIN_REVIEWS):
        """Computes success index using Bayesian weighted rating formula."""
        return (row["review_count"] * row["stars"] + mean_stars * reviews_threshold) / (row["review_count"] + reviews_threshold)


    def density_calculator(self, businesses_gdf):
        """Calculates POI, competitor, transport densities, and success index."""
        pois_gdf = self.load_data_GeoDataFrame(self.poi_path, 'lon', 'lat')

        # Convert to projected CRS for accurate distance calculations
        businesses_gdf = businesses_gdf.to_crs(epsg=3395)
        pois_gdf = pois_gdf.to_crs(epsg=3395)

        # Compute competitor densities
        businesses_gdf["competitor_density"] = businesses_gdf.apply(
            self.calculate_competitor_density, businesses_gdf=businesses_gdf, axis=1
        )

        # Compute densities dynamically
        for category_key in self.categories_dict.keys():
            businesses_gdf[f"{category_key}_density"] = businesses_gdf.apply(
                self.calculate_poi_density, pois_gdf=pois_gdf, axis=1, category_key=category_key
            )

        # Compute success index
        businesses_gdf["success_index"] = businesses_gdf.apply(self.calculate_success_index,
                                                               mean_stars=businesses_gdf["stars"].mean(), axis=1)

        # Drop stars & review_count before returning
        businesses_gdf.drop(columns=["stars", "review_count"], inplace=True)

        return businesses_gdf.to_crs(epsg=4326)