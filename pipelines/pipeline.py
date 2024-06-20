import json
import sys
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy import Nominatim
from ratelimiter import RateLimiter
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Initialize the rate limiter to make at most 1 request per second
rate_limiter = RateLimiter(max_calls=1, period=1)

# Get the Wikipedia URL and Azure Storage account key from environment variables
URL = os.getenv("URL")
ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")


# Function to fetch the HTML content of a Wikipedia page
def get_wikipedia_page(url):
    try:
        # Make a GET request to the Wikipedia page
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses
        return response.text  # Return the HTML content of the page
    except requests.RequestException as e:
        print(f"Error occurred: {e}")


# Function to parse the HTML and extract table rows from the Wikipedia page
def get_wikipedia_data(html):
    soup = BeautifulSoup(html, "html.parser")  # Parse the HTML content using BeautifulSoup
    table = soup.find("table", {"class": "wikitable sortable"})  # Find the target table with class 'wikitable sortable'
    table_rows = table.find_all("tr")  # Get all rows of the table
    return table_rows


# Function to clean and format text by removing unwanted characters
def clean_text(text):
    text = str(text).strip()  # Convert text to string and strip leading/trailing whitespace
    if text.find("[") != -1:
        text = text.split("[")[0]  # Remove any text after the first '[' character
    return text.replace("\n", "")  # Remove newline characters


# Function to get the latitude and longitude of a stadium using geopy
def get_lat_long(stadium, city):
    geolocator = Nominatim(user_agent="get location")  # Initialize geopy Nominatim geolocator
    location = geolocator.geocode(f"{city}, {stadium}")  # Geocode the location using city and stadium name
    if location:
        return location.latitude, location.longitude  # Return the latitude and longitude if found
    return None  # Return None if location not found


# Function to extract and process data from Wikipedia
def extract_wikipedia_data(**kwargs):
    url = kwargs["url"]
    html = get_wikipedia_page(url)  # Fetch the HTML content of the Wikipedia page
    rows = get_wikipedia_data(html)  # Extract table rows from the HTML content

    data = []  # Initialize an empty list to store the data
    for i in range(1, len(rows)):  # Loop through each row, starting from the second row (skipping the header)
        table_data = rows[i].find_all("td")  # Find all cells (td elements) in the row
        if len(table_data) < 9:
            continue  # Skip rows with insufficient columns

        # Extract image URL if available, otherwise set to "No Image"
        image_url = "No Image"
        if len(table_data) >= 9 and table_data[8].find("img"):
            image_url = "https://" + table_data[8].find("img").get("src").split("//")[1]

        # Create a dictionary with the extracted and cleaned data
        values = {
            "rank": i,
            "stadium": clean_text(table_data[1].text),
            "capacity": table_data[2].text.replace(",", ""),  # Remove commas from capacity
            "city": table_data[3].text,
            "state": table_data[4].text,
            "year_opened": table_data[5].text,
            "type": table_data[6].text,
            "tenant(s)": table_data[7].text,
            "image": image_url
        }
        data.append(values)  # Append the dictionary to the data list

    # Convert the data list to a JSON string
    json_rows = json.dumps(data)
    # Push the JSON data to XCom with the key "rows"
    kwargs["ti"].xcom_push(key="rows", value=json_rows)
    return "OK"


# Function to transform the extracted data, including geolocation and data cleaning
def transform_wikipedia_data(**kwargs):
    # Pull the JSON data from XCom using the key "rows"
    data = kwargs["ti"].xcom_pull(key="rows", task_ids="extract_wikipedia_data")
    data = json.loads(data)  # Convert the JSON string back to a Python object

    stadiums_df = pd.DataFrame(data)  # Convert the data to a pandas DataFrame

    # Apply geolocation to each stadium and city, using the rate limiter to avoid overloading the API
    stadiums_df["location"] = stadiums_df.apply(lambda x: rate_limiter(get_lat_long)(x["stadium"], x["city"]), axis=1)
    # Clean and convert the capacity to integer, removing any non-digit characters
    stadiums_df["capacity"] = stadiums_df["capacity"].apply(
        lambda x: int(''.join(filter(str.isdigit, x))) if x else None)

    # Push the transformed data back to XCom
    kwargs["ti"].xcom_push(key="rows", value=stadiums_df.to_json())
    return "OK"


# Function to load the transformed data to Azure Blob Storage
def load_wikipedia_data(**kwargs):
    # Pull the transformed data from XCom using the key "rows"
    data = kwargs["ti"].xcom_pull(key="rows", task_ids="transform_wikipedia_data")
    data = json.loads(data)  # Convert the JSON string back to a Python object

    data = pd.DataFrame(data)  # Convert the data to a pandas DataFrame

    # Create a filename based on the current date and time
    filename = f"stadium_cleaned_{str(datetime.now().date())}_{str(datetime.now().time()).replace(':', '_')}.csv"
    # Save the DataFrame to Azure Blob Storage as a CSV file
    data.to_csv("abfs://wikipedia@wikipediaflow.dfs.core.windows.net/data/" + filename,
                storage_options={
                    "account_key": ACCOUNT_KEY
                }, index=False)
