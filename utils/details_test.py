import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random
import csv
import os

class PropertyDetailScraper:
    def __init__(self, input_file, output_file, delay=(1, 3)):
        """
        input_file: CSV with at least a 'link' column
        output_file: CSV where enriched data will be saved
        delay: tuple for random sleep between requests (min, max)
        """
        self.input_file = input_file
        self.output_file = output_file
        self.delay = delay
        self.results = []

    def fetch_page(self, url):
        """Fetch HTML content of a property detail page."""
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"❌ Failed to fetch {url}: {e}")
            return None

    def parse_details(self, soup):
        """Parse all labeled detail sections (general, energy, characteristics)."""
        data = {}
        for row in soup.select("div.row"):
            label_elem = row.select_one("div.label")
            value_elem = row.select_one("div.value")
            if label_elem and value_elem:
                key = label_elem.get_text(strip=True)
                value = value_elem.get_text(strip=True)
                norm_key = self.normalize_key(key)
                data[norm_key] = self.clean_value(norm_key, value)
        return data

    def normalize_key(self, key):
        """Convert French/label keys into clean ML-friendly names."""
        mapping = {
            "Nombre de chambres": "bedrooms",
            "Nombre de salle de bain": "bathrooms",
            "Salles de bain": "bathrooms",
            "Surface habitable": "living_area",
            "Surface du terrain": "land_area",
            "Année de construction": "year_built",
            "État du bâtiment": "building_condition",
            "Revenu cadastral": "cadastral_income",
            "PEB": "energy_performance",
            "Consommation spécifique d'énergie primaire": "epc_consumption",
            "Terrasse": "terrace",
            "Jardin": "garden",
            "Piscine": "swimming_pool",
        }
        return mapping.get(key, key.lower().replace(" ", "_"))

    def clean_value(self, key, value):
        """Normalize values for ML (Oui/Non → True/False, numbers cleaned)."""
        if value in ["Oui", "yes", "oui"]:
            return True
        if value in ["Non", "no", "non", "-"]:
            return False

        # Remove units (m², kWh, €) → keep only numbers
        if "area" in key or "surface" in key or "consumption" in key:
            return "".join([c for c in value if c.isdigit() or c == "."])

        return value

    def scrape_url(self, url):
        """Scrape a single property URL and return structured data."""
        html = self.fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        data = self.parse_details(soup)

        # Add metadata
        data["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["link"] = url
        return data

    def run(self):
        """Scrape all URLs from the input CSV and save enriched data."""
        with open(self.input_file, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)

        fieldnames = reader.fieldnames.copy()

        for i, row in enumerate(rows, 1):
            url = row["link"]
            print(f"[{i}/{len(rows)}] Scraping {url} ...")
            details = self.scrape_url(url)
            if details:
                row.update(details)
                for key in details.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

            self.results.append(row)
            time.sleep(random.uniform(*self.delay))

        with open(self.output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.results)

        print(f"✅ Scraped {len(self.results)} properties and saved to {self.output_file}")


# --- Example usage ---
if __name__ == "__main__":
    input_file = "./data/properties_noduplicates.csv"
    output_file = "./data/properties_enriched.csv"

    scraper = PropertyDetailScraper(input_file, output_file, delay=(1, 3))
    scraper.run()
