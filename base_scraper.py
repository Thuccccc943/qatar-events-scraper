from abc import ABC, abstractmethod
from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from models import Event
import pandas as pd
import os


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
        """Append new events to CSV, avoiding duplicates based on title + start_date + location"""
        if not filename:
            filename = f"{self.source_name}_events.csv"

        if not events:
            print(f"No events to save for {self.source_name}")
            return

        # Convert to DataFrame for easier handling
        new_df = pd.DataFrame([e.to_dict() for e in events])

        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)

            # Define uniqueness based on title + start_date + location
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.drop_duplicates(
                subset=["title", "start_date", "location"], inplace=True
            )
        else:
            combined_df = new_df

        combined_df.to_csv(filename, index=False, encoding="utf-8")
        print(f"Updated {filename} with {len(combined_df)} total events.")
