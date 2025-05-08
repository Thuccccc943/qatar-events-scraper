from typing import List, Dict, Optional
from models import Event
from base_scraper import BaseScraper
import re


class QatarMuseumsScraper(BaseScraper):
    def __init__(self):
        super().__init__("qmuseums")
        self.base_url = "https://qm.org.qa/en/calendar/?page={page_num}"

    def scrape_events(self, pages: int = 1) -> List[Event]:
        all_events = []
        upperbound = 1
        for page in range(1, pages + 1):
            print(f"Scraping page {page}...")
            url = self.base_url.format(page_num=page)
            # We've reached the last page
            if page > upperbound:
                break
            try:
                response = self.make_request(url)
                soup = self.parse_html(response.content)
                pages = [
                    int(a.get_text())
                    for a in soup.select(".number-button__span")
                    if a.get_text().isdigit()
                ]
                upperbound = max(pages) if pages else 1

                # Find all event cards
                event_cards = soup.find_all("a", class_="card--landscape")

                for card in event_cards:
                    try:
                        event_data = self.extract_event_from_card(card)
                        if event_data:
                            event = self.transform_event(event_data)
                            all_events.append(event)
                    except Exception as e:
                        print(f"Error processing event card: {e}")
            except Exception as e:
                print(f"Error scraping page {page}: {e}")

        return all_events

    def extract_event_from_card(self, card) -> Optional[Dict]:
        try:
            # Extract basic information
            link = card["href"]
            title = (
                self.clean_text(card.find("p", class_="card__title").text)
                if card.find("p", class_="card__title")
                else "No title"
            )
            category = (
                self.clean_text(card.find("p", class_="card__pre-title").text)
                if card.find("p", class_="card__pre-title")
                else "No category"
            )

            # Extract date information (leave unparsed)
            date_text = ""
            date_div = card.find("div", class_="richtext--simple")
            if date_div and date_div.find("p"):
                date_text = self.clean_text(date_div.find("p").text)

            # Extract location
            location = "No location"
            location_tag = card.find("span", class_="museum-tag__span")
            if location_tag:
                location = self.clean_text(location_tag.text)

            # Extract image URL
            image_url = None
            img_tag = card.find("img", class_="picture__image")
            if img_tag and img_tag.has_attr("src"):
                image_url = img_tag["src"]

            return {
                "title": title,
                "category": category,
                "date_text": date_text,  # Leaving date unparsed
                "location": location,
                "link": link,
                "image_url": image_url,
            }
        except Exception as e:
            print(f"Error extracting event from card: {e}")
            return None

    def transform_event(self, raw_event: Dict) -> Event:
        """Transform raw event data into standardized Event object"""
        return Event(
            title=raw_event["title"],
            start_date=raw_event.get(
                "date_text", ""
            ),  # Using date_text for both start and end
            end_date=raw_event.get("date_text", ""),  # since we're not parsing dates
            location=raw_event["location"],
            link=raw_event["link"],
            image_url=raw_event.get("image_url", ""),
            category=raw_event.get("category", ""),
            source=self.source_name,
        )

    def clean_text(self, text: str) -> str:
        """Clean text by normalizing whitespace"""
        if not text:
            return ""
        return " ".join(text.strip().split())
