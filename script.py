#!/usr/bin/env python
from scrapers.iloveqatar import ILoveQatarScraper
from scrapers.visitqatar import VisitQatarScraper
from scrapers.qatarmuseums import QatarMuseumsScraper
from models import Event
from typing import List
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

####### Configuration #######
# Choose which scrapers to run
# You can remove a scraper by prefixing it with #, eg #ILoveQatarScraper(),
scrapers = [
    ILoveQatarScraper(2),
    VisitQatarScraper(),
    QatarMuseumsScraper(2),
]

# Allows you to save results for each source as well
save_individual_results = False
save_to_google_sheets = True


####### Google Sheets setups #######
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open("EventScrapes")

worksheet_names = ["ILoveQatar", "VisitQatar", "QatarMuseums"]
worksheets = {}

for name in worksheet_names:
    try:
        worksheets[name] = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        worksheets[name] = spreadsheet.add_worksheet(title=name, rows="1000", cols="20")

try:
    worksheets["Combined"] = spreadsheet.worksheet("Combined")
except gspread.exceptions.WorksheetNotFound:
    worksheets["Combined"] = spreadsheet.add_worksheet(
        title="Combined", rows="1000", cols="20"
    )


####### Function Definitions #######
def run_scrapers(scrapers: list) -> List[Event]:
    all_events = []
    for scraper in scrapers:
        try:
            print(f"\n{'=' * 50}")
            print(f"Running {scraper.source_name} scraper...")
            events = scraper.scrape_events()
            if save_to_google_sheets:
                events_df = pd.DataFrame([event.to_dict() for event in events])
                worksheet = worksheets[scraper.source_name]
                append_new_events_to_sheet(events_df, worksheet)
            all_events.extend(events)
            print(f"Found {len(events)} events from {scraper.source_name}")

            # Save individual scraper results
            if save_individual_results:
                scraper.save_to_csv(events)
        except Exception as e:
            print(f"Error with {scraper.source_name} scraper: {e}")

    # Update combined worksheet after all scrapers run
    if save_to_google_sheets and all_events:
        combined_df = pd.DataFrame([event.to_dict() for event in all_events])
        append_new_events_to_sheet(combined_df, worksheets["Combined"])

    return all_events


def append_new_events_to_sheet(events_df, worksheet):
    # Sanitize any list-type values
    def sanitize(value):
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        return value

    events_df = events_df.copy()
    events_df["unique_key"] = (
        events_df["title"]
        + events_df["start_date"]
        + events_df["location"]
        + events_df["source"]
    )
    sanitized_df = events_df.applymap(sanitize)

    # Get existing records to detect duplicates
    existing_records = worksheet.get_all_records()
    if existing_records:
        existing_df = pd.DataFrame(existing_records)
        existing_df["unique_key"] = (
            existing_df["title"]
            + existing_df["start_date"]
            + existing_df["location"]
            + existing_df["source"]
        )
        new_events_df = sanitized_df[
            ~sanitized_df["unique_key"].isin(existing_df["unique_key"])
        ]
    else:
        new_events_df = sanitized_df
        # Write headers if sheet is empty
        worksheet.update([list(events_df.columns[:-1])], range_name="A1")

    if new_events_df.empty:
        print(f"No new events to add to {worksheet.title}.")
        return

    # Insert new rows at the top (after headers)
    insert_rows = new_events_df.drop(columns=["unique_key"]).values.tolist()

    # Batch the updates
    existing = worksheet.get_all_values()
    num_new = len(insert_rows)

    # Build the new values (headers + new + existing)
    new_values = [existing[0]] if existing else [list(events_df.columns[:-1])]
    new_values += insert_rows
    if existing:
        new_values += existing[1:]

    # Overwrite the entire sheet in one write
    worksheet.update(new_values)
    print(f"Inserted {num_new} new events into {worksheet.title}.")


####### Run #######
all_events = run_scrapers(scrapers)
