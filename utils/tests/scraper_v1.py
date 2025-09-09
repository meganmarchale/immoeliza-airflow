import requests
from bs4 import BeautifulSoup
import time
import random
import csv
from datetime import datetime
import os

class PropertyScraper:
    def __init__(self, base_url, property_types, output_file="properties.csv"):
        self.base_url = base_url
        self.property_types = property_types
        self.output_file = output_file
        self.results = []

    def fetch_page(self, url):
        """Fetch HTML content of a given page URL."""
        print(f"Fetching: {url}")
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code != 200:
            print(f"Failed to fetch page: {url}, status: {response.status_code}")
            return None
        return response.text

    def parse_page(self, html, property_type):
        """Extract property data from a single page HTML."""
        soup = BeautifulSoup(html, "html.parser")

        section = soup.find("section", id="search-results")
        if not section:
            return []

        cards = section.find_all("h2", class_="card-title ellipsis pr-2 mt-1 mb-0")
        data = []

        for card in cards:
            link_tag = card.find("a")
            if not link_tag:
                continue

            # Link
            link = link_tag.get("href", "").strip()

            # Address (postal code + locality)
            address_tag = card.find_next("p", class_="mb-1 wp-75")
            postal_code, locality = None, None
            if address_tag:
                postal = address_tag.find("span", itemprop="postalCode")
                loc = address_tag.find("span", itemprop="addressLocality")
                if postal:
                    postal_code = postal.get_text(strip=True)
                if loc:
                    locality = loc.get_text(strip=True)

            # Price
            price_tag = address_tag.find_next("strong", class_="list-item-price") if address_tag else None
            price = price_tag.get_text(strip=True) if price_tag else None

            # Timestamp
            scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            data.append({
                "link": link,
                "postal_code": postal_code,
                "locality": locality,
                "price": price,
                "scraped_at": scraped_at,
                "property_type": property_type
            })

        return data

    def save_to_csv(self, data, write_header=False):
        """Save a batch of results into a CSV file."""
        fieldnames = ["link", "postal_code", "locality", "price", "scraped_at", "property_type"]
        with open(self.output_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerows(data)
        print(f"Saved {len(data)} rows to {self.output_file}")

    def run(self):
        """Main scraping loop across all property types and pages."""
        # Check if file exists to determine if we should write header
        first_write = not os.path.exists(self.output_file)

        for prop_type in self.property_types:
            page = 1
            while True:
                url = f"{self.base_url}{page}"
                html = self.fetch_page(url)
                if not html:
                    break

                # You can now detect property_type from the link if needed
                data = self.parse_page(html, property_type=None)
                if not data:
                    print(f"No more data at page {page}. Stopping.")
                    break

                self.save_to_csv(data, write_header=first_write)
                first_write = False

                print(f"Page {page} scraped and saved.")
                page += 1
                time.sleep(random.uniform(2, 5))

# --- usage ---
script_dir = os.path.dirname(os.path.abspath(__file__))  # utils/
data_file = os.path.join(script_dir, "..", "data", "properties.csv")
data_file = os.path.abspath(data_file)  # absolute path

if __name__ == "__main__":
    BASE_URL = (
        "https://immovlan.be/fr/immobilier?transactiontypes=a-vendre,en-vente-publique&propertytypes=maison,appartement&propertysubtypes=maison,villa,immeuble-mixte,bungalow,fermette,maison-de-maitre,chalet,chateau,appartement,rez-de-chaussee,penthouse,studio,duplex,loft,triplex&page="
    )

    scraper = PropertyScraper(
        base_url=BASE_URL,
        output_file=data_file
    )
    scraper.run()
