from scrapers.iloveqatar import ILoveQatarScraper
from scrapers.visitqatar import VisitQatarScraper
from models import Event
from typing import List
import csv
from datetime import datetime


def run_scrapers() -> List[Event]:
    scrapers = [ILoveQatarScraper(), VisitQatarScraper()]

    all_events = []
    for scraper in scrapers:
        try:
            print(f"\n{'=' * 50}")
            print(f"Running {scraper.source_name} scraper...")
            events = scraper.scrape_events()
            all_events.extend(events)
            print(f"Found {len(events)} events from {scraper.source_name}")

            # Save individual scraper results
            scraper.save_to_csv(events)
        except Exception as e:
            print(f"Error with {scraper.source_name} scraper: {e}")

    return all_events


def save_combined_csv(events: List[Event], filename: str = None):
    if not events:
        print("No events to save")
        return

    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"combined_events_{timestamp}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=Event.get_field_names())
        writer.writeheader()
        for event in events:
            writer.writerow(event.to_dict())
    print(f"\nSaved {len(events)} events to {filename}")


def display_stats(events: List[Event]):
    if not events:
        print("No events to display")
        return

    print("\nEvent Statistics:")
    print(f"Total events: {len(events)}")

    sources = {}
    categories = {}
    for event in events:
        sources[event.source] = sources.get(event.source, 0) + 1
        if event.category:
            for cat in event.category.split(", "):
                categories[cat] = categories.get(cat, 0) + 1

    print("\nBy source:")
    for source, count in sources.items():
        print(f"- {source}: {count}")

    print("\nBy category:")
    for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        print(f"- {category}: {count}")


if __name__ == "__main__":
    print("Starting event scraping...")
    events = run_scrapers()
    save_combined_csv(events)
    display_stats(events)
    print("\nScraping complete!")
