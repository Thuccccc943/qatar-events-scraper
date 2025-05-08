import json
from typing import List, Dict, Optional
from models import Event
from base_scraper import BaseScraper


class VisitQatarScraper(BaseScraper):
    def __init__(self):
        super().__init__("VisitQatar")
        self.base_url = "https://visitqatar.com/intl-en/events-calendar/all-events"

    def scrape_events(self) -> List[Event]:
        try:
            response = self.make_request(self.base_url)
            soup = self.parse_html(response.text)

            # Extract the raw events data from the vq-event-listing tag
            event_listing_tag = soup.find("vq-event-listing")
            if not event_listing_tag:
                print("Could not find the 'vq-event-listing' tag on the page.")
                return []

            raw_events_data = event_listing_tag.get(":events")
            raw_events_data = str(raw_events_data)[1:-1]
            if not raw_events_data:
                print(
                    "The ':events' attribute was not found on the vq-event-listing tag."
                )
                return []

            # Clean and parse the raw data
            cleaned_data = self.clean_raw_data(raw_events_data)
            event_list = json.loads(f"[{cleaned_data}]")

            return [self.transform_event(event) for event in event_list if event]

        except Exception as e:
            print(f"Error scraping visitqatar events: {e}")
            return []

    def clean_raw_data(self, raw_data: str) -> str:
        """Clean the raw events data string"""
        # Remove surrounding quotes if present
        if raw_data.startswith("'") and raw_data.endswith("'"):
            raw_data = raw_data[1:-1]

        # Replace HTML entities
        replacements = {
            "&#34;": '"',
            "&amp;": "&",
            "&nbsp;": " ",
            "&lt;": "<",
            "&gt;": ">",
            "&#39;": "'",
            "\n": "",
        }

        for old, new in replacements.items():
            raw_data = raw_data.replace(old, new)

        return raw_data

    def transform_event(self, raw_event: Dict) -> Event:
        """Transform raw event data into standardized Event object"""
        # Format dates
        start_date = raw_event.get("startDate", {})
        end_date = raw_event.get("endDate", {})

        start_date_str = (
            f"{start_date.get('day', '?')} {start_date.get('monthAndYear', '?')}"
        )
        end_date_str = f"{end_date.get('day', '?')} {end_date.get('monthAndYear', '?')}"

        # Format time if available
        time_info = raw_event.get("time", {})
        time_str = None
        start_time = end_time = None

        if isinstance(time_info, dict):
            time_str = time_info.get("formatted12Hour", "")
            if " - " in time_str:
                start_time, end_time = time_str.split(" - ", 1)

        # Format categories
        categories = raw_event.get("category", [])
        category_str = ", ".join(categories) if categories else None

        # Format location
        location = raw_event.get("location", "Location not specified")
        if isinstance(location, dict):
            location = location.get("name", "Location not specified")

        return Event(
            title=raw_event.get("title", "No Title"),
            start_date=start_date_str,
            end_date=end_date_str,
            time=time_str,
            start_time=start_time,
            end_time=end_time,
            location=location,
            description=raw_event.get("description", "")
            .replace("<p>", "")
            .replace("</p>", "")
            .strip(),
            directions=raw_event.get("linkToDirections", {}).get("path", None),
            category=category_str,
            price="Free" if raw_event.get("free", False) else "Paid",
            link=raw_event.get("linkToDetailPage", {}).get("url", "#"),
            source=self.source_name,
            raw_data={"original_data": raw_event, "categories": categories},
        )
