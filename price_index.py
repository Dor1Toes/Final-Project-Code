import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from itertools import count

AVG_PRICES = os.path.join(os.getcwd(), "datasets", "UK_house_prices", "Raw_data", "Average-prices-2024-12.csv")
PRICE_PAIDS = os.path.join(os.getcwd(), "datasets", "UK_house_prices", "Raw_data", "price_paid_data")
PRICE_FILES = ["pp-2022.csv", "pp-2023.csv", "pp-2024.csv"]

YELP_DATA = os.path.join(os.getcwd(), "datasets", "Yelp_API_data", "Raw_data", "Yelp_API_London.csv")

PRICE_PER_ZIP = os.path.join(os.getcwd(), "datasets", "UK_house_prices", "UK_price_per_zip.csv")

OUTPUT_PATH = os.path.join(os.getcwd(), "datasets", "Yelp_API_data", "London.csv")

row_counter = count(1)  # 全局计数器


def load_price_data():
    dataframes = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            file: executor.submit(pd.read_csv, os.path.join(PRICE_PAIDS, file), **{
                "encoding": "utf-8",
                "encoding_errors": "replace",
                "usecols": [1, 2, 3, 13],
                "header": None,
                "names": ["Price", "Date", "postal_code", "Region_Name"]
            })
            for file in PRICE_FILES
        }

        for file, future in futures.items():
            try:
                df = future.result()
                dataframes.append(df)
            except Exception as e:
                print(f"Error loading {file}: {e}")

    price_df = pd.concat(dataframes, ignore_index=True)
    price_df["Date"] = pd.to_datetime(price_df["Date"])
    price_df["Region_Name"] = price_df["Region_Name"].str.lower()
    return price_df


def load_avg_data():
    return pd.read_csv(AVG_PRICES, **{
        "encoding": "utf-8",
        "encoding_errors": "replace",
        "usecols": ["Date", "Region_Name", "Average_Price"]
    }).assign(
        Date=lambda df: pd.to_datetime(df["Date"]),
        Region_Name=lambda df: df["Region_Name"].str.lower()
    )


def adjust_price(row, avg_prices_df, avg_today):
    region = row.Region_Name
    date = row.Date
    price = row.Price

    avg_history_df = avg_prices_df[
        (avg_prices_df["Date"] <= date) & (avg_prices_df["Region_Name"] == region)
        ].sort_values("Date").tail(1)

    if avg_history_df.empty or region not in avg_today:
        return None

    return price * (avg_today[region] / avg_history_df.iloc[0]["Average_Price"])


def price_index_calculator():
    avg_prices_df = load_avg_data()
    today_avg_price = avg_prices_df.query("Date == '2024/12/1'").set_index("Region_Name")["Average_Price"]

    price_df = load_price_data()

    price_df["Adjusted_Price"] = [
        adjust_price(row, avg_prices_df, today_avg_price)
        for row in tqdm(price_df.itertuples(index=False), total=len(price_df), desc="Adjusting Prices")
    ]

    latest_avg_price_per_postal = price_df.groupby("postal_code")["Adjusted_Price"].mean().reset_index()
    latest_avg_price_per_postal.to_csv("UK_price_per_zip.csv", index=False, encoding="utf-8")
    return latest_avg_price_per_postal


if __name__ == "__main__":
    # postal_price_match = price_index_calculator()
    postal_price_match = pd.read_csv(PRICE_PER_ZIP, encoding='utf-8', encoding_errors='replace')

    businesses_df = pd.read_csv(YELP_DATA, **{"encoding": "utf-8", "encoding_errors": "replace"})

    # Merge dataframes on postal_code (left join to keep all businesses)
    df_merged = businesses_df.merge(postal_price_match, on="postal_code", how="left")

    # Handling missing values in Adjusted_Price (options: fill with 0, mean, or drop)
    df_merged["Adjusted_Price"].fillna(postal_price_match["Adjusted_Price"].mean(),
                                       inplace=True)  # Fill missing prices with mean

    df_merged.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(df_merged.head())
