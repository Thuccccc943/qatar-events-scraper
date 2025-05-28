#!/usr/bin/env python
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import CellFormat  # For parsing format data
# import re # Uncomment if you use regex in prepare_key_component


# Function to prepare string components for the unique key
def prepare_key_component(value: any) -> str:
    """
    Normalizes a string for key generation:
    - Converts to string
    - Strips leading/trailing whitespace
    - Converts to lowercase
    - Removes various apostrophe/quote characters
    """
    s = str(value).strip().lower()
    # Characters to remove for key generation
    chars_to_remove = ["'", "’", "‘", "`"]  # Straight, curly (right/left), and backtick
    for char in chars_to_remove:
        s = s.replace(char, "")

    # Optional: More aggressive stripping (uncomment if needed)
    # s = s.replace(" ", "") # Removes all spaces
    # s = re.sub(r'[^\w]', '', s) # Keeps only letters and numbers (requires `import re`)
    return s


def deduplicate_combined_sheet_batched(worksheet_name):
    """
    Deduplicates the "Combined" sheet using batch operations for reads and writes
    to respect API quotas.
    Keeps the entry where Column A has highlighting or content.
    """
    # ###### Google Sheets Setups #######
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_path = "../credentials.json"
    spreadsheet_name = "Event Scrapes"

    print(
        f"Attempting to connect to Google Sheet: '{spreadsheet_name} -> {worksheet_name}'..."
    )
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(spreadsheet_name)
        worksheet = spreadsheet.worksheet(worksheet_name)
        print("Successfully connected.")
    except Exception as e:
        print(f"🛑 Error connecting to Google Sheets: {e}")
        print(
            f"Please ensure '{creds_path}' is correct and has necessary API permissions."
        )
        return

    try:
        print("Fetching all values from the worksheet (1 API call)...")
        all_values = worksheet.get_all_values()
        if not all_values or len(all_values) < 1:
            print(f"⚠️ Worksheet '{worksheet_name}' is empty or has no header row.")
            return
        print(f"Fetched {len(all_values)} total rows (including header).")
    except Exception as e:
        print(f"🛑 Error fetching values from worksheet '{worksheet_name}': {e}")
        return

    header_row = all_values[0]
    data_rows = all_values[1:]
    num_data_rows = len(data_rows)

    if num_data_rows == 0:
        print("✅ No data rows to process.")
        return

    try:
        title_col_idx = header_row.index("title")
        start_date_col_idx = header_row.index("start_date")
        location_col_idx = header_row.index("location")
        source_col_idx = header_row.index("source")
        col_a_idx = 0
        print(
            f"Key columns identified: Title (idx {title_col_idx}), Date (idx {start_date_col_idx}), Location (idx {location_col_idx}), Source (idx {source_col_idx})."
        )
    except ValueError:
        print(
            "🛑 Error: One or more key columns ('title', 'start_date', 'location', 'source') not found in the header row."
        )
        print(f"Actual header row found: {header_row}")
        return

    # Batch fetch formats for Column A (data rows only)
    col_a_formats_map = {}  # Maps gspread_row_index (1-based) -> CellFormat object
    if num_data_rows > 0:
        print(
            f"Batch fetching formats for Column A (A2:A{num_data_rows + 1}) (1 API call)..."
        )
        try:
            # Range for data rows in column A: A2 to A(num_data_rows + 1 because header is row 1)
            range_A_data_rows = f"{worksheet.title}!A2:A{num_data_rows + 1}"

            # Fetch metadata including effective formats for the specified range in column A
            # 'startRow' in the response is 0-indexed and refers to the first row of the *range* requested.
            metadata = worksheet.spreadsheet.fetch_sheet_metadata(
                params={
                    "fields": "sheets/data(rowData(values/effectiveFormat),startRow)",
                    "ranges": [range_A_data_rows],
                }
            )

            if metadata and metadata.get("sheets"):
                sheet_data_list = metadata["sheets"][0].get("data", [])
                if sheet_data_list:
                    # API 'startRow' is 0-indexed. If our range was A2:..., startRow in response should be 1.
                    api_start_row_0_indexed = sheet_data_list[0].get("startRow", 0)
                    row_data_from_metadata = sheet_data_list[0].get("rowData", [])

                    for offset, row_item_format_data in enumerate(
                        row_data_from_metadata
                    ):
                        # Calculate the 1-based gspread row index this format belongs to
                        gspread_row_idx = api_start_row_0_indexed + offset + 1
                        format_dict = {}
                        if (
                            row_item_format_data
                            and row_item_format_data.get("values")
                            and len(row_item_format_data["values"]) > 0
                        ):
                            format_dict = row_item_format_data["values"][0].get(
                                "effectiveFormat", {}
                            )
                        col_a_formats_map[gspread_row_idx] = CellFormat.from_props(
                            format_dict
                        )
            print(
                f"Successfully fetched and parsed formats for {len(col_a_formats_map)} cells in Column A."
            )
        except Exception as e:
            print(
                f"⚠️ Warning: Could not batch fetch formats for Column A. Error: {e}. Formatting checks will be skipped (assuming no highlight)."
            )
            # col_a_formats_map will remain empty; .get() will default to basic CellFormat later.

    processed_events = {}
    print(f"\nProcessing {num_data_rows} data rows to identify duplicates...")
    for idx, row_data in enumerate(data_rows):
        current_row_gspread_idx = idx + 2

        try:
            title = row_data[title_col_idx]
            start_date = row_data[start_date_col_idx]
            location = row_data[location_col_idx]
            source = row_data[source_col_idx]
            col_a_value = row_data[col_a_idx] if len(row_data) > col_a_idx else ""
        except IndexError:
            print(
                f"⚠️ Warning: Row {current_row_gspread_idx} (data index {idx}) has insufficient columns. Skipping."
            )
            continue

        event_key = (
            prepare_key_component(title)
            + prepare_key_component(start_date)
            + prepare_key_component(location)
            + prepare_key_component(source)
        )

        has_col_a_highlight = False
        # Get pre-fetched format; default to an empty format if not found
        cell_format = col_a_formats_map.get(
            current_row_gspread_idx, CellFormat.from_props({})
        )

        if cell_format and cell_format.backgroundColorStyle:
            rgb_color_obj = cell_format.backgroundColorStyle.rgbColor
            if (
                hasattr(rgb_color_obj, "red")
                or hasattr(rgb_color_obj, "green")
                or hasattr(rgb_color_obj, "blue")
            ):
                r = rgb_color_obj.red if hasattr(rgb_color_obj, "red") else 0.0
                g = rgb_color_obj.green if hasattr(rgb_color_obj, "green") else 0.0
                b = rgb_color_obj.blue if hasattr(rgb_color_obj, "blue") else 0.0
                alpha = rgb_color_obj.alpha if hasattr(rgb_color_obj, "alpha") else 1.0

                is_opaque_white = r == 1.0 and g == 1.0 and b == 1.0 and alpha == 1.0
                is_fully_transparent = alpha == 0.0

                if not is_opaque_white and not is_fully_transparent:
                    has_col_a_highlight = True

        row_info = {
            "original_row_index": current_row_gspread_idx,
            "data": row_data,
            "has_col_a_highlight": has_col_a_highlight,
            "col_a_value": col_a_value,
        }
        if event_key not in processed_events:
            processed_events[event_key] = []
        processed_events[event_key].append(row_info)

    rows_to_delete_indices = []
    print(
        "\nDetermining which duplicate rows to keep or delete based on preferences..."
    )
    for event_key, rows_info_list in processed_events.items():
        if len(rows_info_list) <= 1:
            continue

        def sort_key_for_duplicates(row_item):
            highlight_score = 1 if row_item["has_col_a_highlight"] else 0
            col_a_has_content = 1 if str(row_item["col_a_value"]).strip() else 0
            return (
                -highlight_score,
                -col_a_has_content,
                row_item["original_row_index"],
            )

        rows_info_list.sort(key=sort_key_for_duplicates)
        for i in range(1, len(rows_info_list)):
            rows_to_delete_indices.append(rows_info_list[i]["original_row_index"])

    if not rows_to_delete_indices:
        print("\n✅ No duplicate rows to delete were found.")
        return

    # Sort indices in descending order. This is crucial for batch_update
    # as requests are processed in order, and deleting from bottom-up avoids index shifts
    # if the API were to process them sequentially *within* the batch and shift.
    # For `deleteDimension` requests in a single batch, the indices should refer to
    # the sheet's state *before* any operations in that batch begin.
    # However, sorting descending remains good practice and ensures correct behavior if
    # one were to revert to non-batch deletes.
    rows_to_delete_indices.sort(reverse=True)

    print(
        f"\nFound {len(rows_to_delete_indices)} rows to delete. Preparing batch delete request (1 API call)..."
    )

    delete_requests = []
    for row_idx_to_delete in rows_to_delete_indices:
        # Google Sheets API DeleteDimensionRequest uses 0-indexed rows.
        # endIndex is exclusive.
        delete_requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": worksheet.id,  # Get sheetId from the gspread worksheet object
                        "dimension": "ROWS",
                        "startIndex": row_idx_to_delete
                        - 1,  # Convert 1-based gspread index to 0-based API index
                        "endIndex": row_idx_to_delete,  # Deletes the single row at startIndex
                    }
                }
            }
        )

    deleted_count = 0
    failed_to_delete_count = 0
    if delete_requests:
        try:
            print(f"Executing batch delete for {len(delete_requests)} rows...")
            body = {"requests": delete_requests}
            worksheet.spreadsheet.batch_update(body)  # Send the single batch request
            deleted_count = len(
                delete_requests
            )  # Assume all succeed if no exception from batch_update
            print(f"Batch delete request for {deleted_count} rows sent successfully.")
        except Exception as e:
            print(f"🛑 Error during batch delete: {e}")
            failed_to_delete_count = len(
                delete_requests
            )  # Assume all failed in the batch on error
            print(
                "  The sheet may be in a partially deduplicated state. Please check manually or retry."
            )

    print("\n--- Deduplication Summary ---")
    print(
        f"Total rows initially identified for deletion: {len(rows_to_delete_indices)}"
    )
    print(f"Batch delete request sent for: {deleted_count} rows")
    if failed_to_delete_count > 0:
        print(f"Batch delete request failed for: {failed_to_delete_count} rows")
    print("✅ Deduplication process complete.")


if __name__ == "__main__":
    deduplicate_combined_sheet_batched("Combined")
    deduplicate_combined_sheet_batched("ILoveQatar")
    deduplicate_combined_sheet_batched("QatarMuseums")
    deduplicate_combined_sheet_batched("VisitQatar")
