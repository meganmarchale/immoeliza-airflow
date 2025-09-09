import requests
from bs4 import BeautifulSoup
import time
import random
import csv
from datetime import datetime
import os

class PropertyScraper:
    def __init__(self, base_url, output_file="properties.csv"):
        self.base_url = base_url
        self.output_file = output_file
        self.fieldnames = ["link", "postal_code", "locality", "price", "scraped_at", "property_type"]
        self.init_csv()  # ✅ make sure CSV exists with headers
        self.existing_links = self.load_existing_links()

    def init_csv(self):
        """Create CSV with headers if it doesn't exist."""
        if not os.path.exists(self.output_file):
            with open(self.output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writeheader()
            print(f"✅ Created new CSV with headers at {self.output_file}")

    def load_existing_links(self):
        """Load existing links from CSV to avoid duplicates."""
        links = set()
        if os.path.exists(self.output_file):
            with open(self.output_file, newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                if "link" not in reader.fieldnames:
                    print("⚠️ CSV does not have a 'link' column, skipping duplicate check.")
                    return links
                for row in reader:
                    if row.get("link"):
                        links.add(row["link"])
        return links

    def fetch_page(self, url):
        """Fetch HTML content of a given page URL."""
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

    def link_is_valid(self, link):
        """Check if a link is reachable."""
        try:
            resp = requests.head(link, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True, timeout=10)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def parse_page(self, html):
        """Robust parser for property data."""
        soup = BeautifulSoup(html, "html.parser")
        section = soup.find("section", id="search-results")
        if not section:
            return []

        cards = section.find_all("h2", class_=lambda x: x and "card-title" in x)
        data = []

        for card in cards:
            try:
                # Extract link
                link_tag = card.find("a") or card.find_parent("a")
                link = link_tag.get("href", "").strip() if link_tag else None

                if not link: # or link in self.existing_links: <--- Only taking this part out for the first run. Don't forget to delete duplicates after
                    continue  # skip already saved links

                if not self.link_is_valid(link):
                    print(f"Skipping invalid link: {link}")
                    continue

                # Extract address
                address_tag = card.find_next("p")
                postal_code = (address_tag.find("span", itemprop="postalCode").get_text(strip=True)
                               if address_tag and address_tag.find("span", itemprop="postalCode") else None)
                locality = (address_tag.find("span", itemprop="addressLocality").get_text(strip=True)
                            if address_tag and address_tag.find("span", itemprop="addressLocality") else None)

                # Extract price
                price_tag = address_tag.find_next("strong", class_=lambda x: x and "price" in x) if address_tag else None
                price = price_tag.get_text(strip=True) if price_tag else None

                # Extract property type from the link if possible
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

                # Add to existing_links to avoid duplicates during this run
                self.existing_links.add(link)

            except Exception as e:
                print(f"Failed to parse a card: {e}")
                continue

        return data

    def save_to_csv(self, data):
        """Save a batch of results into a CSV file."""
        if not data:
            return
        with open(self.output_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writerows(data)
        print(f"Saved {len(data)} rows to {self.output_file}")

    def run(self):
        """Main scraping loop across pages until no more pages are available."""
        page = 55
        while True:  # run indefinitely
            url = f"{self.base_url}{page}"
            html = self.fetch_page(url)
            if not html:
                print(f"Page {page} could not be fetched. Stopping.")
                break

            data = self.parse_page(html)
            if not data:
                print(f"No more properties found at page {page}. Stopping.")
                break

            self.save_to_csv(data)
            print(f"Page {page} scraped and saved.")
            page += 1
            time.sleep(random.uniform(2, 5))  # polite delay

# --- usage ---
if __name__ == "__main__":
    BASE_URL = (
        "https://immovlan.be/fr/immobilier?transactiontypes=a-vendre,en-vente-publique"
        "&propertytypes=maison,appartement"
        "&propertysubtypes=maison,villa,immeuble-mixte,bungalow,fermette,"
        "maison-de-maitre,chalet,chateau,appartement,rez-de-chaussee,"
        "penthouse,studio,duplex,loft,triplex&page="
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(script_dir, "..", "data", "properties.csv")
    data_file = os.path.abspath(data_file)

    scraper = PropertyScraper(
        base_url=BASE_URL,
        output_file=data_file
    )
    scraper.run()  # for testing:scraper.run(max_pages=2)
