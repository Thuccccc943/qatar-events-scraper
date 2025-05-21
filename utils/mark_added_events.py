import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
from gspread_formatting import *

# ####### Google Sheets setups #######
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("../credentials.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open("Event Scrapes")
worksheet = spreadsheet.worksheet("Working Copy")

# 1. Read the CSV data
csv_titles = set()
csv_file_path = "events.csv"  # Make sure this path is correct
with open(csv_file_path, mode="r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        csv_titles.add(
            row["title_en"].strip()
        )  # .strip() to remove leading/trailing whitespace

print(f"Titles loaded from CSV: {csv_titles}")

# 2. Get all values from the Google Sheet
all_sheet_values = worksheet.get_all_values()

if not all_sheet_values:
    print("Google Sheet is empty. Exiting.")
    exit()

# 3. Find the 'title' column in the Google Sheet
header_row = all_sheet_values[0]
title_col_index = -1
try:
    title_col_index = header_row.index("title")
except ValueError:
    print("Error: 'title' column not found in the Google Sheet header.")
    exit()

print(f"'title' column found at index: {title_col_index}")

# Define the green format
green_format = CellFormat(
    backgroundColor=Color(0.678, 0.886, 0.733)  # A pleasant green (e.g., light green)
)

updates_to_apply = []

# 4. Iterate through rows and compare titles
# Start from the second row to skip the header
for row_idx, row_data in enumerate(all_sheet_values[1:], start=1):
    if len(row_data) > title_col_index:  # Ensure the row has enough columns
        sheet_title = row_data[title_col_index].strip()
        if sheet_title in csv_titles:
            # If title matches, add the first column cell to the list of cells to format
            # gspread uses 1-based indexing for rows and columns
            cell_to_format_a1 = f"A{row_idx + 1}"  # +1 because row_idx is 0-based relative to all_sheet_values[1:]
            # and we need 1-based sheet row number
            updates_to_apply.append(cell_to_format_a1)
            print(
                f"Match found: '{sheet_title}' in row {row_idx + 1}. Adding A{row_idx + 1} to format list."
            )

if updates_to_apply:
    print(f"Applying green formatting to {len(updates_to_apply)} cells in column A...")
    # 5. Batch update cell formatting
    # format_cell_ranges expects a list of tuples: (range, format)
    # We create a list of (cell_address, green_format) for all matched cells
    ranges_to_format = [(cell_range, green_format) for cell_range in updates_to_apply]
    format_cell_ranges(worksheet, ranges_to_format)
    print("Formatting complete!")
else:
    print("No matching titles found in the Google Sheet. No cells formatted.")
