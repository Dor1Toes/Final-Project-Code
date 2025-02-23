import requests
import os
import sys
import pandas as pd

# API key from https://www.yelp.com/developers/v3/manage_app
API_KEY = "nMKZxvw_F_XYrtEgyVa1jGsR3ks7IAGqNi3bYWjOiuNRvWvYOoOwEYdXI1XsBs1DvKyuXATXRQCq_A2ZcnuByBRCasgI0t5SMral0HBqEq21v33tjSCchXNyLqqyZ3Yx"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# API constants, you shouldn't have to change these.
CATEGORIES_URL = 'https://api.yelp.com/v3/categories'
SEARCH_URL = "https://api.yelp.com/v3/businesses/search"

# Defaults for settings
SEARCH_LIMIT = 50
CATEGORIES = ''
LOCALE = 'en_GB'  # United Kingdom

# Where the data obtained from API will be stored
OUTPUT_PATH = os.path.join(os.getcwd(), "datasets", "Yelp_API_data", "Raw_data")


def get_categories():
    response = None
    url_params = {
        'locale': LOCALE
    }

    try:
        response = requests.get(url=CATEGORIES_URL, headers=HEADERS, params=url_params)
        if response.status_code != 200:
            raise ValueError(f"Error occured while getting categories from {LOCALE}\n BAD CODE: {response.status_code}")
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


def search(term, location):
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
                raise ValueError(f"Error occured while searching {term}/ {location}\n BAD CODE: {response.status_code}")
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
                "RestaurantsPriceRange2": len(biz.get("price", ""))  # Count the number of '£' symbols
            }
            businesses.append(business_info)

        print(f"Retrieved {len(businesses)} businesses so far...")

        # Stop if we get less than 50 (no more data available)
        if len(data["businesses"]) < 50:
            break

    print(f"Retrieved {len(businesses)} businesses from: {term} / {location}")

    return businesses


def yelp_UK_data():
    terms = get_categories()

    # terms_df = pd.DataFrame(terms)
    # terms_df.columns = ['UK_F&B_Term']
    # terms_df.to_csv("UK_f&b_terms.csv",index=False,encoding="utf-8")

    locations = ['London']
    businesses = []

    for location in locations:
        for term in terms:
            try:
                businesses.extend(search(term, location))
            except BaseException:
                continue

        df = pd.DataFrame(businesses)
        df.drop_duplicates(subset='business_id', keep='first')
        print(f" Obtained {len(df)} results from {location}")
        df.to_csv(os.path.join(OUTPUT_PATH, f'Yelp_API_{location}.csv'), index=False, encoding="utf-8")

    print('Search finished!')


if __name__ == '__main__':
    yelp_UK_data()