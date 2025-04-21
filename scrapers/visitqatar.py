import json
from textwrap import fill
import requests
from bs4 import BeautifulSoup

url = "https://visitqatar.com/intl-en/events-calendar/all-events"

raw_events_data = ""

try:
    # Fetch the HTML content
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
    html_content = response.text

    # Parse the HTML content
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the vq-event-listing tag
    # We look for the tag by its name
    event_listing_tag = soup.find("vq-event-listing")

    if event_listing_tag:
        # Extract the value of the ':events' attribute
        # The attribute name is ':events'
        raw_events_data = event_listing_tag.get(":events")

        if raw_events_data:
            print("Successfully extracted raw_events_data.")
            # The raw string data you requested is now in the 'raw_events_data' variable.
            # You can print it to see its content and copy it.
            # print(raw_events_data)
        else:
            print("The ':events' attribute was not found on the vq-event-listing tag.")
            raw_events_data = ""
    else:
        print("Could not find the 'vq-event-listing' tag on the page.")

except requests.exceptions.RequestException as e:
    print(f"Error fetching the page: {e}")
    raw_events_data = ""
except Exception as e:
    print(f"An error occurred during parsing or extraction: {e}")
    raw_events_data = ""


def parse_event_data(raw_events):
    """Parse the raw event data and return a list of formatted events."""
    events = []

    # The raw data appears to be multiple JSON objects separated by commas
    # We need to properly format it as a JSON array
    formatted_data = f"[{raw_events}]"

    try:
        event_list = json.loads(formatted_data)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON data: {e}")
        return events

    for event in event_list:
        # Extract relevant information
        title = event.get("title", "No Title")
        summary = event.get("summary", "No summary available")
        description = (
            event.get("description", "").replace("<p>", "").replace("</p>", "").strip()
        )
        category = (
            ", ".join(event.get("category", []))
            if event.get("category")
            else "Uncategorized"
        )

        # Format dates
        start_date = event.get("startDate", {})
        end_date = event.get("endDate", {})
        start_str = (
            f"{start_date.get('day', '?')} {start_date.get('monthAndYear', '?')}"
        )
        end_str = f"{end_date.get('day', '?')} {end_date.get('monthAndYear', '?')}"

        if start_str == end_str:
            date_str = start_str
        else:
            date_str = f"{start_str} - {end_str}"

        location = event.get("location", "Location not specified")
        directions = event.get("linkToDirections", {}).get("path", "#")
        is_free = "Free" if event.get("free", False) else "Paid"
        link = event.get("linkToDetailPage", {}).get("url", "#")

        # Format the event information
        formatted_event = {
            "title": title,
            "date": date_str,
            "start_date": start_str,
            "end_date": end_str,
            "category": category,
            "location": location,
            "directions": directions,
            "price": is_free,
            "summary": summary,
            "description": description,
            "link": link,
        }

        events.append(formatted_event)

    return events


def display_events(events):
    """Display the events in a pretty format."""
    for i, event in enumerate(events, 1):
        print(f"\n{'=' * 50}")
        print(f"EVENT #{i}: {event['title'].upper()}")
        print(f"{'=' * 50}")
        print(f"📅 Date: {event['date']}")
        print(f"Start Date: {event['start_date']}")
        print(f"End Date: {event['end_date']}")
        print(f"🏷️ Category: {event['category']}")
        print(f"📍 Location: {event['location']}")
        print(f"Directions: {event['directions']}")
        print(f"💰 Admission: {event['price']}")
        print(f"\nℹ️ Summary: {event['summary']}")

        if event["description"]:
            print("\n📝 Description:")
            print(fill(event["description"], width=70))

        print(f"\n🔗 More info: {event['link']}")
        print(f"{'-' * 50}")


if __name__ == "__main__":
    raw_events_data = str(raw_events_data)[1:-1]
    cleaned_data = (
        raw_events_data.replace("&#34;", '"')
        .replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#39;", "'")
        .replace("\n", "")
    )
    parsed_events = parse_event_data(cleaned_data)
    display_events(parsed_events)
