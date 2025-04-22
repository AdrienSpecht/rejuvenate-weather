"""Extract AWN data using the Ambient Weather API and save into csv file."""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Your credentials (replace with your real keys)
API_KEY = "252879246db2422790205d5efae1202278385e9c3c52426587be7ac270d5faf4"
APP_KEY = "f4de5bb19c464a1dbcf06094762af2ff00152bc4ad854af1bc7809968f28f4b1"

# Define station MAC address (you’ll need to get this from your AWN dashboard or API)
MAC_ADDRESS = "C4:5B:BE:FA:2E:F0"  # <-- Replace with your device's MAC address

# API endpoint
url = "https://api.ambientweather.net/v1/devices/" + MAC_ADDRESS

def fetch_data(end):
    params = {
        "apiKey": API_KEY,
        "applicationKey": APP_KEY,
        "limit": 288,
        "endDate": end.replace(microsecond=0).isoformat() + "Z"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print("Error:", response.text)
        return []
    time.sleep(1)   # avoid rate limit
    return response.json()

if __name__ == "__main__":
    print("🌐 Fetching data from Ambient Weather Network...")
    end_date = datetime.utcnow()
    data = []
    days_without_data = 0

    while days_without_data < 10:
        start = end_date - timedelta(days=1)
        print(f"📅 Fetching data between {start} to {end_date.date()}")

        chunk = fetch_data(end_date)
        if chunk:
            data.extend(chunk)
            # Use oldest timestamp to go further back in time
            earliest = min(pd.to_datetime([entry['date'] for entry in chunk]))
            end_date = (earliest - timedelta(seconds=1)).replace(tzinfo=None)
            print(f"✅ Retrieved {len(chunk)} new points")
        else:
            print("❌ No data found in that day")
            end_date -= timedelta(days=1)
            days_without_data += 1

    # Convert to DataFrame
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["date"])
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    df.to_csv("awn_history.csv")
    print(f"\n📦 Done! Retrieved {len(df)} records from {df.index.min()} to {df.index.max()}")