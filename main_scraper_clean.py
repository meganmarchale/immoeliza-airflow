import requests
from bs4 import BeautifulSoup
import time
import random
import csv
from datetime import datetime
import os
import pandas as pd
import unidecode

class PropertyScraper:
    def __init__(self, base_url, postal_codes_csv, output_file="properties.csv"):
        self.base_url = base_url
        self.output_file = output_file
        self.fieldnames = ["link", "postal_code", "locality", "price", "scraped_at", "property_type"]
        self.init_csv()
        self.existing_links = self.load_existing_links()
        self.localities = self.load_localities(postal_codes_csv)

    def init_csv(self):
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        if not os.path.exists(self.output_file):
            with open(self.output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writeheader()
            print(f"Created new CSV with headers at {self.output_file}")

    def load_existing_links(self):
        links = set()
        if os.path.exists(self.output_file):
            with open(self.output_file, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                if "link" in reader.fieldnames:
                    for row in reader:
                        if row.get("link"):
                            links.add(row["link"])
        return links

    def load_localities(self, csv_path):
        df = pd.read_csv(csv_path, dtype=str, sep=None, engine='python')
        df.columns = df.columns.str.strip().str.lower()
        code_col = next((c for c in df.columns if "code" in c), None)
        loc_col = next((c for c in df.columns if "local" in c or "localite" in c), None)
        if not code_col or not loc_col:
            raise ValueError(f"Cannot find 'code' and 'locality' columns in CSV. Found: {df.columns.tolist()}")
        localities = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            locality = str(row[loc_col]).strip().lower()
            slug = unidecode.unidecode(locality.replace(" ", "-"))
            localities.append(f"{code}-{slug}")
        print(f"Loaded {len(localities)} localities")
        return localities

    def fetch_page(self, url):
        try:
            print(f"Fetching: {url}")
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if response.status_code != 200:
                print(f"Failed to fetch page: {url}, status: {response.status_code}")
                return None
            return response.text
        except requests.RequestException as e:
            print(f"Request failed for {url}: {e}")
            return None

    def parse_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        section = soup.find("section", id="search-results")
        if not section:
            return []
        cards = section.find_all("h2", class_=lambda x: x and "card-title" in x)
        data = []
        for card in cards:
            try:
                link_tag = card.find("a") or card.find_parent("a")
                link = link_tag.get("href", "").strip() if link_tag else None
                if not link or link in self.existing_links:
                    continue
                address_tag = card.find_next("p")
                postal_code = (address_tag.find("span", itemprop="postalCode").get_text(strip=True)
                               if address_tag and address_tag.find("span", itemprop="postalCode") else None)
                locality = (address_tag.find("span", itemprop="addressLocality").get_text(strip=True)
                            if address_tag and address_tag.find("span", itemprop="addressLocality") else None)
                price_tag = address_tag.find_next("strong", class_=lambda x: x and "price" in x) if address_tag else None
                price = price_tag.get_text(strip=True) if price_tag else None
                property_type = None
                if link:
                    for subtype in ["villa", "maison", "immeuble-mixte", "bungalow", "fermette",
                                    "maison-de-maitre", "chalet", "chateau", "appartement",
                                    "rez-de-chaussee", "penthouse", "studio", "duplex", "loft", "triplex"]:
                        if subtype in link.lower():
                            property_type = subtype
                            break
                data.append({
                    "link": link,
                    "postal_code": postal_code,
                    "locality": locality,
                    "price": price,
                    "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "property_type": property_type
                })
                self.existing_links.add(link)
            except Exception as e:
                print(f"Failed to parse a card: {e}")
                continue
        return data

    def clean_data(self, rows):
        if not rows:
            return []
        df = pd.DataFrame(rows)
        # Clean price → int64
        if "price" in df.columns:
            df["price"] = (df["price"].astype(str).str.replace(r"[^\d]", "", regex=True)
                            .replace("", pd.NA))
            df["price"] = pd.to_numeric(df["price"], errors="coerce").dropna().astype("int64")
        # Clean terrace/swimming_pool/garage if present
        for col in ["garage", "swimming_pool", "terrace"]:
            if col in df.columns:
                df[col] = df[col].fillna("non").astype(str)
                df[col] = df[col].apply(lambda x: False if str(x).strip().lower() == "non" or str(x).strip() == "" else True)
        # year_built → int
        if "year_built" in df.columns:
            df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce").astype("Int64")
        # living_area → int
        if "living_area" in df.columns:
            df["living_area"] = (df["living_area"].astype(str).str.replace(r"[^\d]", "", regex=True)
                                  .replace("", pd.NA))
            df["living_area"] = pd.to_numeric(df["living_area"], errors="coerce").astype("Int64")
        return df.to_dict(orient="records")

    def save_to_csv(self, data):
        if not data:
            return
        data = self.clean_data(data)
        with open(self.output_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writerows(data)
        print(f"Saved {len(data)} cleaned rows")

    def scrape_locality(self, locality, max_pages=None):
        page = 1
        consecutive_existing = 0
        while True:
            url = f"{self.base_url}&towns={locality}&page={page}"
            html = self.fetch_page(url)
            if not html:
                break
            data = self.parse_page(html)
            if not data:
                print(f"No more properties in {locality} (page {page})")
                break
            new_data = []
            for row in data:
                if row["link"] in self.existing_links:
                    consecutive_existing += 1
                    print(f"Skipping existing link ({consecutive_existing} in a row)")
                else:
                    new_data.append(row)
                    consecutive_existing = 0
            if consecutive_existing >= 5:
                print(f"Stopping {locality} early (5 existing links in a row)")
                break
            self.save_to_csv(new_data)
            print(f"{locality} - page {page} scraped ({len(new_data)} new rows)")
            page += 1
            if max_pages and page > max_pages:
                break
            time.sleep(random.uniform(0.5, 1.5))

    def run(self, max_pages=None):
        start_time = time.time()
        for locality in self.localities:
            print(f"Scraping locality: {locality}")
            self.scrape_locality(locality, max_pages=max_pages)
            print(f"Finished {locality}")
        end_time = time.time()
        elapsed = end_time - start_time
        print("\n⏱️ Scraping completed in {:.2f} seconds".format(elapsed))
        print(f"📊 Total rows added: {self.total_rows_added}")


if __name__ == "__main__":
    BASE_URL = (
        "https://immovlan.be/fr/immobilier"
        "?transactiontypes=a-vendre,en-vente-publique"
        "&propertytypes=maison,appartement"
        "&propertysubtypes=maison,villa,immeuble-mixte,bungalow,fermette,"
        "maison-de-maitre,chalet,chateau,appartement,rez-de-chaussee,"
        "penthouse,studio,duplex,loft,triplex"
    )
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(script_dir, ".", "data", "quick_test.csv")
    data_file = os.path.abspath(data_file)
    postal_codes_csv = os.path.join(script_dir, ".", "data", "code-postaux-belge.csv")
    scraper = PropertyScraper(
        base_url=BASE_URL,
        postal_codes_csv=postal_codes_csv,
        output_file=data_file
    )
    scraper.run(max_pages=50)
