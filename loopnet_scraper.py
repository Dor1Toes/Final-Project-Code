from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd


def create_driver():
    driver_path = 'chromedriver.exe'
    # Set up Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Uncomment to run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    print("Initializing Chrome WebDriver...")
    driver = webdriver.Chrome(service=Service(driver_path), options=chrome_options)
    print("Chrome WebDriver initialized successfully.")
    return driver

def scrape_property_data(base_url):
    """Scrape property listings from multiple pages until no 'Next Page' button exists."""

    driver = create_driver()
    driver.get(base_url)

    all_property_data = []

    while True:
        time.sleep(5)  # Allow page to load

        # Extract property listings
        listings = driver.find_elements(By.TAG_NAME, "article")

        for listing in listings:
            try:
                property_area = listing.find_element(By.XPATH, ".//div[@class='header-col header-left']").text
                city_address = listing.find_element(By.XPATH, ".//a[@class='right-h6']").text
                size = listing.find_element(By.XPATH, ".//a[@class='right-h4']").text

                try:
                    price = listing.find_element(By.XPATH, ".//ul[@class='data-points-a']/li[@name='Price']").text
                except:
                    price = "N/A"

                try:
                    availability = listing.find_element(By.XPATH,
                                                        ".//ul[@class='data-points-a']/li[@name='SpaceAvailable']").text
                except:
                    availability = "N/A"

            except:
                try:
                    title = listing.find_element(By.XPATH, ".//h4/a").text
                except:
                    title = "N/A"

                try:
                    subtitle = listing.find_element(By.XPATH, ".//h6/a").text
                except:
                    subtitle = ""

                property_area = f"{title}\n{subtitle}".strip(", ")

                try:
                    city_address = listing.find_element(By.XPATH, ".//a[@class='subtitle-beta']").text
                except:
                    city_address = "N/A"

                try:
                    size = listing.find_element(By.XPATH, ".//ul[@class='data-points-2c']/li[1]").text
                except:
                    size = "N/A"

                try:
                    price = listing.find_element(By.XPATH, ".//ul[@class='data-points-2c']/li[@name='Price']").text
                except:
                    price = "N/A"

                try:
                    availability = listing.find_element(By.XPATH,
                                                        ".//ul[@class='data-points-2c']/li[@name='SpaceAvailable']").text
                except:
                    availability = "N/A"

            all_property_data.append({
                "property_area": property_area,
                "city_address": city_address,
                "size": size,
                "price": price,
                "availability": availability
            })

        # Try to find the "Next Page" button
        try:
            next_page_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@data-automation-id='NextPage']"))
            )
            next_page_button.click()  # Click to go to the next page
            time.sleep(5)  # Allow new listings to load

        except:
            print("No more pages to scrape. Exiting loop.")
            break

    # Save data to CSV
    df = pd.DataFrame(all_property_data)
    df.to_csv("loopnet_property_list_Tucson.csv", index=False)

    driver.quit()
    print(f"Scraping completed. Extracted {len(all_property_data)} listings and saved to 'loopnet_property_list.csv'.")

url='https://www.loopnet.ca/search/restaurants/az--usa/for-lease/?sk=ac604e173ef4cf5b8d2a68e8963af9c2&bb=-5oi6lk_3Lr162knhB'
scrape_property_data(url)
