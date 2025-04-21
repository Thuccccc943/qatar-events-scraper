from abc import ABC, abstractmethod
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from models import Event


class BaseScraper(ABC):
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def make_request(self, url: str) -> requests.Response:
        """Common method for making HTTP requests"""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            raise

    def parse_html(self, content: str) -> BeautifulSoup:
        """Common method for parsing HTML"""
        return BeautifulSoup(content, "html.parser")

    @abstractmethod
    def scrape_events(self) -> List[Event]:
        """Main method to be implemented by each scraper"""
        pass

    def save_to_csv(self, events: List[Event], filename: str = None):
        """Save events to CSV with source-specific filename if not provided"""
        if not filename:
            filename = f"{self.source_name}_events.csv"

        if not events:
            print(f"No events to save for {self.source_name}")
            return

        import csv

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=Event.get_field_names())
            writer.writeheader()
            for event in events:
                writer.writerow(event.to_dict())
        print(f"Saved {len(events)} events to {filename}")
