import requests
import json
import os
import argparse
from datetime import datetime, timezone, timedelta

# API URL and constants
URL_API = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/donnees-synop-essentielles-omm/exports/json"
HEADERS = {"accept": "*/*"}

def get_date_range(year=None, month=None, day=None):
    """
    Calculate the date range for the specified year, month, and day.
    If no arguments are provided, return the range for yesterday.
    """
    if year and month and day:
        date_start = datetime(year, month, day, tzinfo=timezone.utc)
        date_end = date_start + timedelta(days=1) - timedelta(seconds=1)
    elif year and month:
        date_start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            date_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
        else:
            date_end = datetime(year, month + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
    else:
        # Default to yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    
    return date_start.isoformat(), date_end.isoformat()

def fetch_data_from_api(url, params, headers):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def save_data_to_file(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    print(f"Data saved to '{file_path}'.")

def load_data_from_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

def count_data_elements(data):
    return len(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and save data from the API based on specified date.")
    parser.add_argument("--year", type=int, help="The year for the data (e.g., 2025).")
    parser.add_argument("--month", type=int, choices=range(1, 13), help="The month for the data (1 to 12).")
    parser.add_argument("--day", type=int, choices=range(1, 32), help="The day for the data (1 to 31).")
    args = parser.parse_args()

    # Get date range
    date_start, date_end = get_date_range(args.year, args.month, args.day)
    print(f"Fetching data for range: {date_start} to {date_end}")

    # File path
    # Construire le nom du dossier avec le format attendu
    folder_name = f"{args.year}/{args.month:02d}"

    # Vérifiez si le jour est spécifié, sinon utilisez le format mois-année
    if args.day is None:
        # Si 'day' n'est pas spécifié, nom du fichier : YYYY-MM.json
        json_file_name = f"/opt/airflow/data/raw/{folder_name}/{args.year}-{args.month:02d}.json"
    else:
        # Si 'day' est spécifié, nom du fichier : YYYY-MM-DD.json
        json_file_name = f"/opt/airflow/data/raw/{folder_name}/{date_start.split('T')[0]}.json"
    
    # Query parameters
    params = {
        "where": f"date >= '{date_start}' AND date <= '{date_end}'",
        "timezone": "UTC",
        "use_labels": "false",
        "epsg": 4326,
    }

    # Fetch and save data
    data = fetch_data_from_api(URL_API, params, HEADERS)
    if data:
        save_data_to_file(data, json_file_name)
        print(f"Number of elements: {count_data_elements(data)}")