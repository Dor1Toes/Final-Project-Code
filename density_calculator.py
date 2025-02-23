import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os
from datetime import datetime

# Define POI categories
POI_CATEGORY_DICT = {
    "transport": {"stop_position", "bus_station", "bus_stop", "tram_stop", "halt", "station", "taxi"},
    "shopping": {"supermarket", "mall", "department_store"},
    "education": {"school", "university", "college", "library"},
    "healthcare": {"hospital", "clinic", "veterinary", "pharmacy"},
}

# Global rating average & review threshold
C = 3.5  # Assume average rating is 3.5
m = 50  # Minimum reviews to consider rating reliable


def load_data_GeoDataFrame(file_path, row_lon, row_lat):
    """Loads data from CSV and converts it to a GeoDataFrame."""
    data_df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='replace')

    # Convert to GeoDataFrame
    data_df["geometry"] = data_df.apply(lambda row: Point(row[row_lon], row[row_lat]), axis=1)
    return gpd.GeoDataFrame(data_df, geometry="geometry", crs="EPSG:4326")


def calculate_poi_density(business, pois_gdf, radius, category_key=None):
    """Calculates the number of POIs within a given radius based on a category key."""
    buffer = business.geometry.buffer(radius)

    if category_key:
        valid_types = POI_CATEGORY_DICT.get(category_key, set())
        pois_gdf = pois_gdf[pois_gdf["type"].isin(valid_types)]

    return pois_gdf[pois_gdf.geometry.intersects(buffer)].shape[0]


def calculate_competitor_density(business, businesses_gdf, radius):
    """Calculates the number of competitors around a given business."""
    business_categories = set(business["categories"].split(", "))

    competitors = businesses_gdf[businesses_gdf["categories"].apply(
        lambda x: bool(set(x.split(", ")).intersection(business_categories))
    )]

    competitors = competitors[competitors["business_id"] != business["business_id"]]

    buffer = business.geometry.buffer(radius)
    return competitors[competitors.geometry.intersects(buffer)].shape[0]


def calculate_success_index(row):
    """Computes success index using Bayesian weighted rating formula."""
    return (row["review_count"] * row["stars"] + m * C) / (row["review_count"] + m)


def density_calculator(yelp_path, pois_path, state_code, radius):
    """Calculates POI, competitor, transport densities, and success index."""
    businesses_gdf = load_data_GeoDataFrame(yelp_path, 'longitude', 'latitude')
    businesses_gdf = businesses_gdf[businesses_gdf["state"] == state_code]

    pois_gdf = load_data_GeoDataFrame(pois_path, 'lon', 'lat')

    # Convert to projected CRS for accurate distance calculations
    businesses_gdf = businesses_gdf.to_crs(epsg=3395)
    pois_gdf = pois_gdf.to_crs(epsg=3395)

    # Compute competitor densities
    businesses_gdf["competitor_density"] = businesses_gdf.apply(
        calculate_competitor_density, businesses_gdf=businesses_gdf, radius=radius, axis=1
    )

    # Compute densities dynamically
    for category_key in POI_CATEGORY_DICT.keys():
        businesses_gdf[f"{category_key}_density"] = businesses_gdf.apply(
            calculate_poi_density, pois_gdf=pois_gdf, radius=radius, axis=1, category_key=category_key
        )

    # Compute success index
    businesses_gdf["success_index"] = businesses_gdf.apply(calculate_success_index, axis=1)

    # Drop stars & review_count before returning
    businesses_gdf.drop(columns=["stars", "review_count"], inplace=True)

    return businesses_gdf.to_crs(epsg=4326)


if __name__ == "__main__":
    radius = 1000
    # state_list = ['AZ','DE','ID','IL','IN','LA']
    # state_list = ['MO', 'NJ', 'NV', 'PA', 'TN']
    state_list = ['FL']

    script_dir = os.path.dirname(os.path.abspath(__file__))
    yelp_path = os.path.join(script_dir, "data", "yelp_dataset", "Yelp_with_prices.csv")

    for state in state_list:
        osm_file_path = os.path.join(script_dir, "data", "poi_dataset", "US", state)
        output_path = os.path.join(script_dir, "data", "processed_data", state)

        os.makedirs(output_path, exist_ok=True)

        for file in os.listdir(osm_file_path):
            if file.endswith('.csv'):
                poi_path = os.path.join(osm_file_path, file)

                properties_gdf = density_calculator(yelp_path, poi_path, state, radius)

                # Save results as JSON and CSV
                properties_df = pd.DataFrame(properties_gdf)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
                properties_df.to_csv(os.path.join(output_path, f"{state}_results_{timestamp}.csv"), index=False, encoding="utf-8")

                print(f"{state} data has been extracted !")
