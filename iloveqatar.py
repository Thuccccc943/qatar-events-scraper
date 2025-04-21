import requests
from bs4 import BeautifulSoup


def scrape_events(page_num):
    url = f"https://www.iloveqatar.net/events/sports/p{page_num}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")

    # Assuming events are in a div with a specific class (you might need to adjust this based on the actual HTML structure)
    event_items = soup.find_all("a", class_="article-block__title")

    events = []

    for event in event_items:
        event_url = event["href"]
        event_page = requests.get(event_url, headers=headers)
        event_page_soup = BeautifulSoup(event_page.content, "html.parser")
        title = (
            event_page_soup.find_all("h1")[0].get_text(strip=True)
            if event_page_soup.find_all("h1")[0]
            else "No title"
        )
        print(title)
        date = (
            event_page_soup.find("div", class_="events-page-info__item _date").get_text(
                strip=True
            )
            if event_page_soup.find("div", class_="events-page-info__item _date")
            else "No date"
        )
        time = (
            event_page_soup.find("div", class_="events-page-info__item _time").get_text(
                strip=True
            )
            if event_page_soup.find("div", class_="events-page-info__item _time")
            else "No time"
        )
        location = (
            event_page_soup.find(
                "div", class_="events-page-info__item _location"
            ).get_text(strip=True)
            if event_page_soup.find("div", class_="events-page-info__item _location")
            else "No location"
        )
        tickets_and_prices = event_page_soup.find_all(
            "div", class_="events-page-info__item _tickets"
        )
        tickets = "No tickets"
        prices = "No prices"
        if len(tickets_and_prices) > 0:
            tickets = tickets_and_prices[0].get_text(strip=True)
        if len(tickets_and_prices) > 1:
            prices = tickets_and_prices[1].get_text(strip=True)

        events.append(
            {
                "title": title,
                "date": date,
                "time": time,
                "location": location,
                "tickets": tickets,
                "prices": prices,
            }
        )

    return events


def main():
    all_events = []
    for page in range(1, 2):  # Loop through the first 20 pages
        print(f"Scraping page {page}")
        events = scrape_events(page)
        if not events:
            print(f"No events found on page {page}.")
        else:
            all_events.extend(events)

    print(f"Scraped {len(all_events)} event(s)")

    import csv

    csv_cols = ["title", "date", "time", "location", "tickets", "prices"]
    csv_file = "events.csv"
    with open(csv_file, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_cols)
        writer.writeheader()
        for data in all_events:
            writer.writerow(data)


if __name__ == "__main__":
    main()
