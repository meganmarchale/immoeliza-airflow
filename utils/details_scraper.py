import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random
import csv

class PropertyDetailScraper:
    def __init__(self, urls, delay=(1, 3)):
        """
        urls: list of property URLs to scrape
        delay: tuple for random sleep between requests (min, max)
        """
        self.urls = urls
        self.delay = delay
        self.results = []

    def fetch_page(self, url):
        """Fetch the HTML content of a property detail page."""
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return None

    def parse_general_info(self, soup):
        """Parse the general info section into a dictionary."""
        data = {}
        general_info = soup.select_one("div.general-info.w-100")
        if not general_info:
            return data

        for row in general_info.select("div.row"):
            label_elem = row.select_one("div.label")
            value_elem = row.select_one("div.value")
            if label_elem and value_elem:
                key = label_elem.get_text(strip=True)
                value = value_elem.get_text(strip=True)
                data[key] = value

        return data

    def parse_energy_info(self, soup):
        """Parse energy / heating info if present."""
        data = {}
        energy_section = soup.select_one("div.energy-info")
        if not energy_section:
            return data

        for row in energy_section.select("div.row"):
            label_elem = row.select_one("div.label")
            value_elem = row.select_one("div.value")
            if label_elem and value_elem:
                key = label_elem.get_text(strip=True)
                value = value_elem.get_text(strip=True)
                data[key] = value

        return data

    def scrape_url(self, url):
        """Scrape a single property URL and return structured data."""
        html = self.fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        data = {}
        data.update(self.parse_general_info(soup))
        data.update(self.parse_energy_info(soup))

        # Add timestamp and URL
        data["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["url"] = url
        return data

    def run(self):
        """Scrape all URLs in the list."""
        for url in self.urls:
            print(f"Scraping: {url}")
            details = self.scrape_url(url)
            if details:
                self.results.append(details)
            time.sleep(random.uniform(*self.delay))

        print(f"Scraped {len(self.results)} properties.")
        return self.results


# --- Example usage ---
if __name__ == "__main__":
    urls = []
    with open("./data/properties_noduplicates.csv", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
                    urls.append(row["link"])  # pick the "link" column
                    if i == 9:  # stop after 10 links (0–9)
                        break

    scraper = PropertyDetailScraper(urls)
    properties = scraper.run()

    for prop in properties:
        for k, v in prop.items():
            print(f"{k}: {v}")
        print("-" * 40)
