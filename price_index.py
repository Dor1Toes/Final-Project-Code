import os
import pandas as pd

AVG_PRICES = os.path.join(os.getcwd(), "data", "house_prices", "UK", "Average-prices-2024-12.csv")
PRICE_PAIDS = os.path.join(os.getcwd(), "data", "house_prices", "UK", "price_paid_data")
PRICE_FILES = ["pp-2022.csv", "pp-2023.csv", "pp-2024.csv"]
YELP_DATA = os.path.join(os.getcwd(), "data", "house_prices", "UK", "Average-prices-2024-12.csv")
OUTPUT_PATH = os.path.join(os.getcwd(), "data", "house_prices", "UK", "Average-prices-2024-12.csv")

def load_price_data():
    dataframes = []  # Store individual DataFrames

    for file in PRICE_FILES:
        file_path = os.path.join(PRICE_PAIDS, file)
        df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='replace',
                         usecols=[1, 2, 3, 13], header=None, names=['Price', 'Date', 'postal_code', 'Region_Name'])
        dataframes.append(df)

    # Combine all DataFrames into one
    price_df = pd.concat(dataframes, ignore_index=True)

    # Convert Date column to datetime and standardize Region_Name
    price_df['Date'] = pd.to_datetime(price_df['Date'])
    price_df['Region_Name'] = price_df['Region_Name'].str.lower()

    return price_df


def load_avg_data():
    # Load the historical regional average price data
    avg_prices_df = pd.read_csv(AVG_PRICES, encoding='utf-8', encoding_errors='replace',
                                usecols=['Date', 'Region_Name', 'Average_Price'])
    avg_prices_df['Date'] = pd.to_datetime(avg_prices_df['Date'])
    avg_prices_df['Region_Name'] = avg_prices_df['Region_Name'].str.lower()

    return avg_prices_df


def price_index_calculator():
    # Load the historical regional average price data
    avg_prices_df = load_avg_data()

    today_avg_price = avg_prices_df[avg_prices_df['Date'] == '2024/12/1'].set_index('Region_Name')['Average_Price']

    # Load the historical property price data
    price_df = load_price_data()

    price_df['Adjusted_Price'] = price_df.apply(adjust_price, avg_prices_df=avg_prices_df, avg_today=today_avg_price, axis=1)

    # Compute latest average price per postal code
    latest_avg_price_per_postal = price_df.groupby('postal_code')['Adjusted_Price'].mean().reset_index()

    return latest_avg_price_per_postal


# Function to adjust price
def adjust_price(row, avg_prices_df, avg_today):
    region = row['Region_Name']
    date = row['Date']
    price = row['Price']

    # Find historical HPI
    avg_history = avg_prices_df[(avg_prices_df['Date'] <= date) & (avg_prices_df['Region_Name'] == region)]
    if avg_history.empty:
        return None  # No matching HPI data

    avg_history = avg_history.sort_values('Date').iloc[-1]['Average_Price']  # Latest before purchase date
    hpi_today_value = avg_today.get(region, None)  # Current HPI

    if pd.isna(avg_history) or pd.isna(hpi_today_value):
        return None  # Missing HPI data

    # Compute adjusted price
    return price * (hpi_today_value / avg_history)


if __name__ == '__main__' :
    postal_price_match = price_index_calculator()

    businesses_df = pd.read_csv(YELP_DATA,encoding='utf-8', encoding_errors='replace')

    # Merge the business data with property price data on postal_code
    merged_df = businesses_df.merge(postal_price_match, on="postal_code", how="left")

    # Save the merged data
    merged_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(merged_df.head())