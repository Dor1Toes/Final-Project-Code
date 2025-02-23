import os
import json
import pandas as pd
from datetime import datetime


script_dir = os.path.dirname(os.path.abspath(__file__))

yelp_dataset = script_dir + "\data\yelp_dataset\yelp_academic_dataset_business.json"
US_zip_prices = script_dir + "\data/house_prices/US/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"


# Back up into CSV for checking
def backup_to_csv(df, file_name="Backup"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
    filename = script_dir + f'/backup\{file_name}_{timestamp}.csv'
    df.to_csv(filename, index=False)


# import Yelp f&b data in US and convert to dataframe
def yelp_extract(json_file):
    # Define the categories to filter
    target_categories = {"Restaurants", "Food", "Cafes", "Bakeries", "Bars", "Coffee & Tea", "Fast Food", "Pubs",
                         "Wine Bars"}

    # Open the JSON file in read mode
    with open(json_file, 'r', encoding='utf-8') as f:
        extracted_data = []

        # Read each line in the JSONL file and process it
        for line in f:
            try:
                # Load each line as a separate JSON object
                business = json.loads(line)

                # Extract the "categories" field, check if it's not None
                categories = business.get("categories")
                if categories:
                    if isinstance(categories, str):
                        categories = categories.split(",")  # If categories are in a string, split them by commas
                    categories = set(map(lambda x: x.strip(), categories))

                    # Extract the postal code
                    postal_code = business.get("postal_code", "")

                    review_count = business.get("review_count")

                    state = business.get("state")

                    # Ensure postal_code contains only digits (US ZIP format)
                    if categories & target_categories and postal_code.isdigit() and not state.isdigit() :
                        # Extract the required attributes
                        business_info = {
                            "business_id": business.get("business_id"),
                            "name": business.get("name"),
                            "address": business.get("address"),
                            "city": business.get("city"),
                            "state": state,
                            "postal_code": postal_code,  # Now it's guaranteed to be numeric
                            "latitude": business.get("latitude"),
                            "longitude": business.get("longitude"),
                            "stars": business.get("stars"),
                            "review_count": review_count,
                            "is_open": business.get("is_open"),
                            "categories": business.get("categories"),
                            # Safely access "RestaurantsPriceRange2" with try-except block
                            "RestaurantsPriceRange2": None
                        }
                        try:
                            # Attempt to access "RestaurantsPriceRange2" safely
                            if business.get("attributes"):
                                business_info["RestaurantsPriceRange2"] = business["attributes"].get(
                                    "RestaurantsPriceRange2", None)
                        except Exception as e:
                            print(f"Error accessing RestaurantsPriceRange2: {e}")
                            business_info["RestaurantsPriceRange2"] = None

                        extracted_data.append(business_info)
            except json.JSONDecodeError:
                print("Skipping malformed line.")
                continue

    # Convert extracted data to pandas DataFrame
    df = pd.DataFrame(extracted_data)
    return df





if __name__ == '__main__':
    # Load Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv
    avg_price_df = pd.read_csv(US_zip_prices)

    # Ensure the attributes from avg_price_df is treated as a string (zip code)
    avg_price_df['postal_code'] = avg_price_df['RegionName'].astype(str)
    avg_price_df['ZHVI_index'] = avg_price_df['2024/11/30'].astype(float)

    # Extract US F&B Yelp data from yelp_academic_dataset_business.json
    yelp_df = yelp_extract(yelp_dataset)

    # Merge the business data with the price data based on postal_code and RegionName
    merged_df = pd.merge(yelp_df, avg_price_df[['postal_code', 'ZHVI_index']],
                         how='left', left_on='postal_code', right_on='postal_code')

    # Back up into CSV for checking
    backup_to_csv(merged_df,file_name="Yelp_with_prices")



    print("pause")
