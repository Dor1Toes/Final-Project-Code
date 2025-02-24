import os
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from datetime import datetime

AVG_PRICES = os.path.join(os.getcwd(), "datasets/house_prices/UK/Raw_data/Average-prices-2024-12.csv")

PAID_PRICES = os.path.join(os.getcwd(), "datasets/house_prices/UK/Raw_data/price_paid_data")

PRICE_FILES = ["pp-2022.csv", "pp-2023.csv", "pp-2024.csv"]

PRICES_OUTPUT = os.path.join(os.getcwd(), "datasets/house_prices/UK")


class PriceIndex:

    def __init__(self, avg_prices = AVG_PRICES, paid_prices = PAID_PRICES, price_files = PRICE_FILES, prices_output = PRICES_OUTPUT):
        self.avg_prices = avg_prices
        self.paid_prices = paid_prices
        self.price_files = price_files
        self.prices_output = prices_output

    def load_price_data(self):
        dataframes = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                file: executor.submit(pd.read_csv, os.path.join(self.paid_prices, file), **{
                    "encoding": "utf-8",
                    "encoding_errors": "replace",
                    "usecols": [1, 2, 3, 13],
                    "header": None,
                    "names": ["Price", "Date", "postal_code", "Region_Name"]
                })
                for file in self.price_files
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

    def load_avg_data(self):
        return pd.read_csv(self.avg_prices, **{
            "encoding": "utf-8",
            "encoding_errors": "replace",
            "usecols": ["Date", "Region_Name", "Average_Price"]
        }).assign(
            Date=lambda df: pd.to_datetime(df["Date"]),
            Region_Name=lambda df: df["Region_Name"].str.lower()
        )

    def adjust_price(self, row, avg_prices_df, avg_today):
        region = row.Region_Name
        date = row.Date
        price = row.Price

        avg_history_df = avg_prices_df[
            (avg_prices_df["Date"] <= date) & (avg_prices_df["Region_Name"] == region)
            ].sort_values("Date").tail(1)

        if avg_history_df.empty or region not in avg_today:
            return None

        return price * (avg_today[region] / avg_history_df.iloc[0]["Average_Price"])

    def price_index_calculator(self):
        avg_prices_df = self.load_avg_data()
        today_avg_price = avg_prices_df.query("Date == '2024/12/1'").set_index("Region_Name")["Average_Price"]

        price_df = self.load_price_data()

        price_df["Adjusted_Price"] = [
            self.adjust_price(row, avg_prices_df, today_avg_price)
            for row in tqdm(price_df.itertuples(index=False), total=len(price_df), desc="Adjusting Prices")
        ]

        latest_avg_price_per_postal = price_df.groupby("postal_code")["Adjusted_Price"].mean().reset_index()

        return latest_avg_price_per_postal

    def save_to_csv(self, df):
        # Save the table for merge
        df.to_csv(os.path.join(self.prices_output, f"UK_price_per_zip.csv"), index=False, encoding="utf-8")

        # Backup the result
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
        df.to_csv(os.path.join(self.prices_output, 'Backup', f"UK_price_per_zip_{timestamp}.csv"),
                  index=False,
                  encoding="utf-8")

        print(f'UK_price_per_zip.csv saved to {self.prices_output} at {timestamp}')

    def load_prices_per_zip(self):
        postal_price_match = pd.read_csv(os.path.join(self.prices_output, "UK_price_per_zip.csv"), encoding='utf-8',encoding_errors='replace')
        print(f'Successfully loaded UK_price_per_zip.csv from {self.prices_output}')
        return postal_price_match


if __name__ == "__main__":
    PI = PriceIndex()
    PI.load_prices_per_zip()
    print("finished")
