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
    ILoveQatarScraper(3),
    VisitQatarScraper(),
    QatarMuseumsScraper(3),
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
spreadsheet = client.open("Event Scrapes")

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

try:
    worksheets["Copy"] = spreadsheet.worksheet("Working Copy")
except gspread.exceptions.WorksheetNotFound:
    worksheets["Copy"] = spreadsheet.add_worksheet(
        title="Working Copy", rows="1000", cols="20"
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
        append_new_events_to_sheet(combined_df, worksheets["Copy"])

    return all_events


def append_new_events_to_sheet(events_df: pd.DataFrame, worksheet: gspread.Worksheet):
    if events_df.empty:
        print(f"No new events DataFrame to process for worksheet '{worksheet.title}'.")
        return

    # Sanitize list-type values in the incoming DataFrame
    # It's crucial to operate on a copy to avoid SettingWithCopyWarning
    sanitized_df = events_df.copy()

    def sanitize(value):
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        return value

    for col in sanitized_df.columns:
        sanitized_df[col] = sanitized_df[col].apply(sanitize)

    # Define required columns for the unique key and ensure they exist
    required_key_cols = ["title", "start_date", "location", "source"]
    missing_key_cols = [
        col for col in required_key_cols if col not in sanitized_df.columns
    ]
    if missing_key_cols:
        print(
            f"Warning: DataFrame for '{worksheet.title}' is missing required key columns: {missing_key_cols}. Cannot reliably generate unique keys or check duplicates."
        )
        # Decide behavior: either return, or attempt to proceed without robust duplicate checking.
        # For now, let's allow it to proceed, but unique key will be less effective.
        for col in missing_key_cols:
            sanitized_df[col] = ""  # Add missing key columns as empty strings

    sanitized_df["unique_key"] = (
        sanitized_df["title"].astype(str)
        + sanitized_df["start_date"].astype(str)
        + sanitized_df["location"].astype(str)
        + sanitized_df["source"].astype(str)
    )

    # Get existing data from the sheet
    try:
        all_sheet_values = worksheet.get_all_values()
    except gspread.exceptions.APIError as e:
        print(
            f"Error fetching data from worksheet '{worksheet.title}': {e}. Quota likely exceeded or API issue."
        )
        return

    existing_headers = []
    existing_data_rows_values = []

    if all_sheet_values:
        existing_headers = all_sheet_values[0]
        existing_data_rows_values = all_sheet_values[1:]

    # Handle sheet initialization (if empty or no headers)
    if not existing_headers:
        # Sheet is empty or headerless. Initialize with headers from the current events_df.
        # The 'unique_key' column is temporary and should not be a header in the sheet.
        new_headers = [col for col in sanitized_df.columns if col != "unique_key"]

        if not new_headers:
            print(
                f"Cannot initialize headers for '{worksheet.title}' as sanitized_df (excluding unique_key) has no columns."
            )
            return

        worksheet.update([new_headers], range_name="A1")
        worksheet.freeze(rows=1)  # Freeze the header row
        print(
            f"Initialized headers for empty sheet '{worksheet.title}' and froze the first row."
        )
        existing_headers = new_headers  # Update for current processing

        # All incoming events are considered new as the sheet was empty
        new_events_to_add_df = sanitized_df.copy()
    else:
        # Sheet has headers. Proceed with duplicate checking.
        if not existing_data_rows_values:  # Headers exist, but no data rows
            existing_df_for_keys = pd.DataFrame(columns=existing_headers)
        else:
            existing_df_for_keys = pd.DataFrame(
                existing_data_rows_values, columns=existing_headers
            )

        # Check if required key columns exist in the *sheet's* headers for creating existing unique keys
        sheet_has_key_cols = all(
            col in existing_df_for_keys.columns for col in required_key_cols
        )

        if sheet_has_key_cols and not existing_df_for_keys.empty:
            # Create unique_key for existing sheet data
            existing_df_for_keys["unique_key"] = (
                existing_df_for_keys["title"].astype(str)
                + existing_df_for_keys["start_date"].astype(str)
                + existing_df_for_keys["location"].astype(str)
                + existing_df_for_keys["source"].astype(str)
            )
            # Filter out events that already exist
            new_events_to_add_df = sanitized_df[
                ~sanitized_df["unique_key"].isin(existing_df_for_keys["unique_key"])
            ]
        elif existing_df_for_keys.empty:  # Headers exist, but no data rows
            new_events_to_add_df = sanitized_df.copy()  # All incoming events are new
        else:  # Sheet exists but missing key columns for robust duplicate check
            print(
                f"Warning: Existing sheet '{worksheet.title}' is missing one or more key columns ({required_key_cols}) in its headers. Duplicate check might be incomplete. Assuming incoming events are new if not found by any existing 'unique_key' column."
            )
            if (
                "unique_key" in existing_df_for_keys.columns
            ):  # Check if a 'unique_key' col somehow exists
                new_events_to_add_df = sanitized_df[
                    ~sanitized_df["unique_key"].isin(existing_df_for_keys["unique_key"])
                ]
            else:  # Cannot perform unique key check, assume all are new
                new_events_to_add_df = sanitized_df.copy()

    if new_events_to_add_df.empty:
        print(f"No new events to add to '{worksheet.title}' after duplicate checking.")
        return

    # Prepare rows for insertion, aligning with existing sheet headers
    # This ensures manually added columns in the sheet are respected.
    final_rows_to_insert_values = []
    # Use a copy of new_events_to_add_df, dropping the temporary 'unique_key' column
    # as it should not be written to the sheet itself.
    df_for_insertion = new_events_to_add_df.drop(
        columns=["unique_key"], errors="ignore"
    )

    for _, event_data_series in df_for_insertion.iterrows():
        row_values = []
        for (
            header_name
        ) in existing_headers:  # Iterate in the order of the sheet's current headers
            if header_name in event_data_series:
                row_values.append(event_data_series[header_name])
            else:
                # This column exists in the sheet (e.g., "Review") but not in the new event data
                row_values.append("")  # Add an empty string for this cell
        final_rows_to_insert_values.append(row_values)

    if not final_rows_to_insert_values:
        print(
            f"No event rows prepared for insertion into '{worksheet.title}' (this might happen if all new events were filtered out or column alignment failed)."
        )
        return

    # Insert new rows into the Google Sheet, insert automatically deals with shifting other rows
    try:
        # `insert_rows` is 1-indexed for the `row` parameter.
        # Inserting at row 2 pushes existing content (from row 2 downwards) down.
        worksheet.insert_rows(
            final_rows_to_insert_values, row=2, value_input_option="USER_ENTERED"
        )
        print(
            f"Successfully inserted {len(final_rows_to_insert_values)} new event(s) into '{worksheet.title}' at the top (after headers)."
        )
    except gspread.exceptions.APIError as e:
        print(
            f"API Error inserting rows into '{worksheet.title}': {e}. This could be a quota issue or data format problem."
        )
        # Potentially log more details about final_rows_to_insert_values if error persists
    except Exception as e:
        print(
            f"An unexpected error occurred during row insertion into '{worksheet.title}': {e}"
        )


####### Run #######
all_events = run_scrapers(scrapers)
