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


def append_new_events_to_sheet(events_df: pd.DataFrame, worksheet: gspread.Worksheet):
    if events_df.empty:
        print(f"No new events DataFrame to process for worksheet '{worksheet.title}'.")
        return

    def prepare_key_component(value: any) -> str:
        s = str(value).strip().lower()

        # Characters to remove for key generation
        # Focus on apostrophes and similar quote-like characters as per user feedback
        chars_to_remove = ["'", "’", "‘", "`"]
        for char in chars_to_remove:
            s = s.replace(char, "")
        return s

    # 1. Sanitize incoming DataFrame (for list conversion, etc.)
    sanitized_df = events_df.copy()

    def sanitize_df_values(value):  # Converts lists in cells to strings
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        return value

    for col in sanitized_df.columns:
        sanitized_df[col] = sanitized_df[col].apply(sanitize_df_values)

    required_key_cols = ["title", "start_date", "location", "source"]
    missing_key_cols = [
        col for col in required_key_cols if col not in sanitized_df.columns
    ]
    if missing_key_cols:
        print(
            f"Warning: Incoming DataFrame for '{worksheet.title}' is missing key columns: {missing_key_cols}. Adding as empty strings for key generation."
        )
        for col in missing_key_cols:
            sanitized_df[col] = ""

    # Create unique keys for new events using prepared (stripped) components
    sanitized_df["unique_key"] = (
        sanitized_df["title"].apply(prepare_key_component)
        + sanitized_df["start_date"].apply(prepare_key_component)
        + sanitized_df["location"].apply(prepare_key_component)
        + sanitized_df["source"].apply(prepare_key_component)
    )

    # 2. Get existing data from the sheet
    try:
        all_sheet_cells = worksheet.get_all_values()
    except gspread.exceptions.APIError as e:
        print(
            f"Error fetching data from worksheet '{worksheet.title}': {e}. Quota likely exceeded or API issue."
        )
        return

    sheet_header_row_from_a1 = []
    sheet_data_rows_from_col_a = []

    if all_sheet_cells and all_sheet_cells != [[]]:
        sheet_header_row_from_a1 = all_sheet_cells[0]
        if len(all_sheet_cells) > 1:
            sheet_data_rows_from_col_a = all_sheet_cells[1:]

    data_headers_b_onwards = (
        sheet_header_row_from_a1[1:] if len(sheet_header_row_from_a1) > 0 else []
    )
    new_events_to_add_df = pd.DataFrame()

    # 3. Handle sheet initialization or prepare existing data for comparison
    if not data_headers_b_onwards:  # If B1 onwards is unheadered
        print(
            f"Sheet '{worksheet.title}' has no data headers from B1 onwards. Initializing headers."
        )
        headers_for_b1_onwards = [
            col for col in sanitized_df.columns if col != "unique_key"
        ]
        if not headers_for_b1_onwards:
            print(
                f"Cannot initialize headers for '{worksheet.title}': no data columns in DataFrame (excluding unique_key)."
            )
            return

        worksheet.update([headers_for_b1_onwards], range_name="B1")
        if not sheet_header_row_from_a1:  # If A1 was also empty
            worksheet.update_cell(1, 1, "")
        worksheet.freeze(rows=1)
        print(
            f"Initialized data headers for '{worksheet.title}' from B1 and froze the first row. Column A1 is blank or preserved."
        )

        data_headers_b_onwards = headers_for_b1_onwards
        new_events_to_add_df = sanitized_df.copy()  # All incoming events are new
        existing_sheet_df = pd.DataFrame(
            columns=data_headers_b_onwards
        )  # For consistent flow

    else:  # Sheet has existing data headers from B1 onwards
        data_for_df_b_onwards = []
        for r_idx, row_data_from_a in enumerate(sheet_data_rows_from_col_a):
            actual_row_data_b_onwards = (
                row_data_from_a[1:] if len(row_data_from_a) > 0 else []
            )
            len_diff = len(data_headers_b_onwards) - len(actual_row_data_b_onwards)
            if len_diff > 0:
                actual_row_data_b_onwards.extend([""] * len_diff)
            elif len_diff < 0:
                actual_row_data_b_onwards = actual_row_data_b_onwards[
                    : len(data_headers_b_onwards)
                ]
            data_for_df_b_onwards.append(actual_row_data_b_onwards)

        if not data_for_df_b_onwards:
            existing_sheet_df = pd.DataFrame(columns=data_headers_b_onwards)
        else:
            existing_sheet_df = pd.DataFrame(
                data_for_df_b_onwards, columns=data_headers_b_onwards
            )

        sheet_has_key_cols = all(
            col in existing_sheet_df.columns for col in required_key_cols
        )

        if sheet_has_key_cols and not existing_sheet_df.empty:
            # Ensure key columns are strings before applying preparation
            for col in required_key_cols:
                if col not in existing_sheet_df.columns:
                    existing_sheet_df[col] = ""
                existing_sheet_df[col] = existing_sheet_df[col].astype(str)

            # Create unique keys for existing events using prepared (stripped) components
            existing_sheet_df["unique_key"] = (
                existing_sheet_df["title"].apply(prepare_key_component)
                + existing_sheet_df["start_date"].apply(prepare_key_component)
                + existing_sheet_df["location"].apply(prepare_key_component)
                + existing_sheet_df["source"].apply(prepare_key_component)
            )
            new_events_to_add_df = sanitized_df[
                ~sanitized_df["unique_key"].isin(existing_sheet_df["unique_key"])
            ]
        elif existing_sheet_df.empty:
            new_events_to_add_df = sanitized_df.copy()  # All incoming events are new
        else:
            print(
                f"Warning: Existing sheet '{worksheet.title}' (data from B onwards) is missing one or more key columns ({required_key_cols}) in its headers. Duplicate check might be incomplete."
            )
            # Fallback if unique_key somehow exists (less likely to be prepared/stripped consistently)
            if (
                "unique_key" in existing_sheet_df.columns
                and "unique_key" in sanitized_df.columns
            ):
                new_events_to_add_df = sanitized_df[
                    ~sanitized_df["unique_key"].isin(existing_sheet_df["unique_key"])
                ]
            else:
                new_events_to_add_df = (
                    sanitized_df.copy()
                )  # Assume all new if robust check isn't possible

    if new_events_to_add_df.empty:
        print(f"No new events to add to '{worksheet.title}' after duplicate checking.")
        return

    # 4. Prepare rows for GSpread insertion (using original, non-stripped data for sheet cells)
    final_rows_to_insert = []
    # df_for_insertion contains original (or list-sanitized) data, NOT the key-stripped data
    df_for_insertion = new_events_to_add_df.drop(
        columns=["unique_key"], errors="ignore"
    )

    for _, event_series in df_for_insertion.iterrows():
        row_for_b_onwards = []
        for header_b in (
            data_headers_b_onwards
        ):  # Iterate based on sheet's data headers (B1 onwards)
            # Get the original value for the cell, not the stripped one used in the key
            row_for_b_onwards.append(str(event_series.get(header_b, "")))
        final_row_with_blank_a = [
            ""
        ] + row_for_b_onwards  # Prepend empty string for Column A
        final_rows_to_insert.append(final_row_with_blank_a)

    if not final_rows_to_insert:
        print(f"No event rows prepared for insertion into '{worksheet.title}'.")
        return

    # 5. Insert new rows into Google Sheet
    try:
        worksheet.insert_rows(
            final_rows_to_insert, row=2, value_input_option="USER_ENTERED"
        )
        print(
            f"Successfully inserted {len(final_rows_to_insert)} new event(s) into '{worksheet.title}' (data from Col B, Col A is blank)."
        )
    except gspread.exceptions.APIError as e:
        print(
            f"API Error inserting rows into '{worksheet.title}': {e}. This could be a quota issue or data format problem."
        )
    except Exception as e:
        print(
            f"An unexpected error occurred during row insertion into '{worksheet.title}': {e}"
        )


####### Run #######
all_events = run_scrapers(scrapers)
