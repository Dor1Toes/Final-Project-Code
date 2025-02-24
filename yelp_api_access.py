import requests
import os
import sys
import pandas as pd
import Ipynb_importer
from density_calculator import DensityCalculator
from UK_price_index import PriceIndex
from datetime import datetime

# API key from https://www.yelp.com/developers/v3/manage_app
API_KEY = "nMKZxvw_F_XYrtEgyVa1jGsR3ks7IAGqNi3bYWjOiuNRvWvYOoOwEYdXI1XsBs1DvKyuXATXRQCq_A2ZcnuByBRCasgI0t5SMral0HBqEq21v33tjSCchXNyLqqyZ3Yx"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# API constants, you shouldn't have to change these.
CATEGORIES_URL = 'https://api.yelp.com/v3/categories'
SEARCH_URL = "https://api.yelp.com/v3/businesses/search"

# Defaults for settings
SEARCH_LIMIT = 50
LOCALE = 'en_GB'  # United Kingdom

# Default Location list
LOCATIONS = ['London']

# Path to POI files
POI_PATH = os.path.join(os.getcwd(), "datasets", "POI_data")

# Where the processed data be stored
OUTPUT_PATH = os.path.join(os.getcwd(), "datasets", "Training_data", "UK")


class ApiAccess:
    def __init__(self, locations=LOCATIONS, output_path=OUTPUT_PATH, poi_path=POI_PATH):
        self.output_path = output_path
        self.locations = locations
        self.poi_path = poi_path

    def get_categories(self):
        response = None
        url_params = {
            'locale': LOCALE
        }

        try:
            response = requests.get(url=CATEGORIES_URL, headers=HEADERS, params=url_params)
            if response.status_code != 200:
                raise ValueError(
                    f"Error occured while getting categories from {LOCALE}\n BAD CODE: {response.status_code}")
        except ValueError:
            return

        data = response.json()

        unique_parent_aliases = set(
            parent for category in data['categories'] for parent in category['parent_aliases']
        )

        filtered_titles = [
            category['title']
            for category in data['categories']
            if any(parent in ['food', 'restaurants', 'bars', 'breweries', 'cafes',
                              'breakfast_brunch', 'wineries', 'gourmet'] for parent in category['parent_aliases'])
        ]

        print(f"Discovered {len(filtered_titles)} categories from: {LOCALE}")

        return filtered_titles

    def search(self, term, location):
        formatted_location = location.replace(' ', '+')
        formatted_term = term.replace(' ', '+')
        response = None
        businesses = []
        for offset in range(1, 200, 50):
            url_params = {
                'term': formatted_term,
                'location': formatted_location,
                'limit': SEARCH_LIMIT,
                'offset': offset,
                'locale': LOCALE
            }

            try:
                response = requests.get(url=SEARCH_URL, headers=HEADERS, params=url_params)
                if response.status_code != 200:
                    raise ValueError(
                        f"Error occured while searching {term}/ {location}\n BAD CODE: {response.status_code}")
            except ValueError:
                return

            data = response.json()
            business_data = data.get("businesses", [])

            # Collect data for all businesses
            for biz in business_data:
                business_info = {
                    "business_id": biz.get("id", ""),
                    "name": biz.get("name", ""),
                    "address": biz["location"].get("address1", ""),
                    "city": biz["location"].get("city", ""),
                    "state": biz["location"].get("state", ""),
                    "postal_code": biz["location"].get("zip_code", ""),
                    "latitude": biz["coordinates"].get("latitude", ""),
                    "longitude": biz["coordinates"].get("longitude", ""),
                    "stars": biz.get("rating", ""),
                    "review_count": biz.get("review_count", ""),
                    "is_open": biz.get("is_open_now", ""),
                    "categories": ", ".join([category["title"] for category in biz.get("categories", [])]),
                    "RestaurantsPriceRange": len(biz.get("price", ""))  # Count the number of '£' symbols
                }
                businesses.append(business_info)

            print(f"Retrieved {len(businesses)} businesses so far...")

            # Stop if we get less than 50 (no more data available)
            if len(data["businesses"]) < 50:
                break

        print(f"Retrieved {len(businesses)} businesses from: {term} / {location}")

        return businesses

    def yelp_UK_data(self):
        terms = self.get_categories()
        pIndex = PriceIndex()

        # Backup currently availble UK food&beverage terms
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
        terms_df = pd.DataFrame(terms)
        terms_df.columns = ['UK_F&B_Term']
        terms_df.to_csv(os.path.join(self.output_path, "backup", "terms", f"UK_F&B_terms_{timestamp}.csv"), index=False,
                        encoding="utf-8")

        # For Test
        terms = ['cafe']

        for location in self.locations:
            businesses = []
            for term in terms:
                try:
                    businesses.extend(self.search(term, location))
                except BaseException:
                    continue

            df = pd.DataFrame(businesses)
            df.drop_duplicates(subset='business_id', keep='first')
            print(f" Obtained {len(df)} results from {location}")

            # Backup the raw businesses data
            df.to_csv(os.path.join(self.output_path, "backup", "businesses", f'yelp_{location}_{timestamp}.csv'),
                      index=False, encoding="utf-8")

            # Load the average prices per postal code and merge to business data
            p_zip_df = pIndex.load_prices_per_zip()
            df_merged_prices = df.merge(p_zip_df, on="postal_code", how="left")

            # Calculate the dentsities and merge to business data
            d_calc = DensityCalculator(poi_path=os.path.join(self.poi_path, f"{location}.csv"))
            df_training_data = d_calc.density_calculator(df_merged_prices)

            # Save merged result to training data
            df_training_data.to_csv(os.path.join(self.output_path, f"{location}_for_training.csv"), index=False,
                                    encoding="utf-8")

            # Backup Training data
            df_training_data.to_csv(
                os.path.join(self.output_path, "BackUp", f"{location}_{timestamp}__for_training.csv"), index=False,
                encoding="utf-8")

            print(f'Finished Searching: {location}!')

if __name__ == '__main__':
    YelpAccess = ApiAccess()
    YelpAccess.yelp_UK_data()