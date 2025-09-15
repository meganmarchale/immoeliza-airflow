import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

LABEL_MAPPING = {
    "nombre de chambres": "bedrooms",
    "nombre de salle de bain": "bathrooms",
    "salles de bain": "bathrooms",
    "surface habitable": "living_area",
    "surface du terrain": "land_area",
    "année de construction": "year_built",
    "etat du bien": "building_condition",
    "garage": "garage",
    "consommation spécifique d'énergie primaire": "epc_consumption",
    "terrasse": "terrace",
    "jardin": "garden",
    "piscine": "swimming_pool"
}

class PropertyDetailScraperParallel:
    def __init__(self, input_file, output_file, delay=(1,2), max_workers=5):
        self.input_file = input_file
        self.output_file = output_file
        self.delay = delay
        self.max_workers = max_workers
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    def fetch_page(self, url):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            time.sleep(random.uniform(*self.delay))  # polite delay per thread
            return resp.text
        except Exception as e:
            print(f"❌ Failed {url}: {e}")
            return None

    def parse_details(self, html):
        soup = BeautifulSoup(html, "html.parser")
        data = {}
        for h4 in soup.select("h4"):
            label = h4.get_text(strip=True).lower()
            parent = h4.parent
            if not parent:
                continue
            p = parent.find("p")
            if not p:
                continue
            value = p.get_text(strip=True)
            for key, field in LABEL_MAPPING.items():
                if key in label:
                    data[field] = value
                    break
        return data

    def scrape_row(self, row):
        url = row["link"]
        html = self.fetch_page(url)
        if html:
            details = self.parse_details(html)
            row.update(details)
        if "scraped_at" not in row or not row["scraped_at"]:
            row["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return row

    def run(self):
        with open(self.input_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # combine old + new fieldnames
        fieldnames = list(rows[0].keys())
        for f in LABEL_MAPPING.values():
            if f not in fieldnames:
                fieldnames.append(f)
        if "scraped_at" not in fieldnames:
            fieldnames.append("scraped_at")

        with open(self.output_file, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_row = {executor.submit(self.scrape_row, row): row for row in rows}
                for i, future in enumerate(as_completed(future_to_row), 1):
                    enriched_row = future.result()
                    writer.writerow(enriched_row)
                    f_out.flush()
                    print(f"[{i}/{len(rows)}] Saved {enriched_row.get('link')}")

        print(f"✅ Finished scraping {len(rows)} properties → {self.output_file}")


if __name__ == "__main__":
    input_file = "./data/properties_noduplicates.csv"
    output_file = "./data/properties_enriched_parallel.csv"

    scraper = PropertyDetailScraperParallel(input_file, output_file, max_workers=10)
    scraper.run()
