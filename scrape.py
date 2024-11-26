import requests
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from db_table import db_table
import json

START_DATE = "01/01/2019"
END_DATE = "08/13/2024"   #why is the end date assigned a fixed value. Should not it be datetime.now() so that everytime code runs, it fetches data until today

# ID used for the AJAX request to get the data
commodityType = {
    "GOLD": 8830,
    "SILVER": 8836
}

# Some needed headers to get the request working
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'text/plain, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest'
}


def create_payload(start, end, interval, type_):

    if not interval or not type_:
        raise RuntimeError("Invalid interval or type_")
    start = urllib.parse.quote(start, safe='')
    end = urllib.parse.quote(end, safe='')
    payload = 'curr_id=' + str(commodityType[type_]) + '&st_date=' + start + \
              '&end_date=' + end + \
              '&interval_sec=Daily' + interval + \
              '&sort_col=date&sort_ord=DESC&action=historical_data'
    return payload


def get_gold_data(start_date, end_date):
    payload = create_payload(start_date, end_date, "DAILY", "GOLD")
    response = requests.post("https://www.investing.com/instruments/HistoricalDataAjax",
                             data=payload,
                             headers=headers)
    return response.text


def get_silver_data(start_date, end_date):
    payload = create_payload(start_date, end_date, "DAILY", "SILVER")
    response = requests.post("https://www.investing.com/instruments/HistoricalDataAjax",
                             data=payload,
                             headers=headers)
    return response.text

# code to read gold data
gold_data = None
with open('gold.json') as f:
    gold_data = json.load(f)

# code to read silver data
silver_data = None
with open('silver.json') as f:
    silver_data = json.load(f)


# Iniit the database connection
db_schema = {
    "Date": "BIGINT PRIMARY KEY",
    "Gold": "float",
    "Silver": "float"
}

db = db_table("Prices", db_schema)

# # Used to store the data so we can cleanly insert into the Database.
prices = {}

# for row in gold_data.find_all('td', class_="first left bold noWrap"):
for row in gold_data["data"]:
    date = datetime.strptime(row["rowDate"], '%b %d, %Y').replace(
        tzinfo=timezone.utc).timestamp()
    price = str(row["last_close"]).replace(",", "")
    prices[date] = {"Gold": price}


for row in silver_data["data"]:
    date = datetime.strptime(row["rowDate"], '%b %d, %Y').replace(
        tzinfo=timezone.utc).timestamp()
    price = str(row["last_close"]).replace(",", "")
    if date not in prices:  # Could be that there are no gold data for the current date. If so, give gold value of null
        prices[date] = {"Silver": price}
    else:
        prices[date].update({"Silver": price})

# create the
for date in prices:
    newItem = {
        "Date": date
    }
    newItem.update(prices[date])
    db.insert(newItem)

print("Done updating database")
