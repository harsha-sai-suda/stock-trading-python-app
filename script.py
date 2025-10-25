import requests
import os
import csv

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

def run_stock_job():
    # GET data from Polygon.io API using GET request
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data['results']:
        tickers.append(ticker)

    # Handle paginated API responses
    while 'next_url' in data:
        print('requesting next page...', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        if 'error' in data:
            print('error: ', data['error'])
            break
        #print(data)
        for ticker in data['results']:
            tickers.append(ticker)

    # Example ticker data
    example_ticker = {'ticker': 'HSBH',
       'name': 'HSBC Holdings plc ADRhedged',
       'market': 'stocks', 
       'locale': 'us', 
       'primary_exchange': 'ARCX', 
       'type': 'ETS', 
       'active': True, 
       'currency_name': 'usd', 
       'composite_figi': 'BBG01Q37TG78', 
       'share_class_figi': 'BBG01Q37THL0', 
       'last_updated_utc': '2025-10-12T06:05:08.375181062Z'}

    print(len(tickers))

    # Write tickers to CSV with same schema as example_ticker
    csv_columns = list(example_ticker.keys())
    csv_file = "tickers.csv"

    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for ticker in tickers:
            # Ensure all keys exist, fill missing with empty string
            row = {key: ticker.get(key, "") for key in csv_columns}
            writer.writerow(row)

    print(f'Wrote {len(tickers)} tickers to {csv_file}')

if __name__ == "__main__":
    run_stock_job()