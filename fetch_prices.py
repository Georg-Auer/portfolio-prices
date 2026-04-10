#!/usr/bin/env python3
"""
Fetch stock prices using yfinance
Queries Databricks for ticker list, then fetches prices directly
Saves prices to prices/latest_prices.csv
"""

import os
import requests
import json
import time
import pandas as pd
import yfinance as yf
from datetime import datetime

# Get credentials from environment variables (set in GitHub Secrets)
DATABRICKS_HOST = os.environ['DATABRICKS_HOST'].rstrip('/')  # Remove trailing slash
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']
DATABRICKS_WAREHOUSE_ID = os.environ['DATABRICKS_WAREHOUSE_ID']

# Configuration
CATALOG = 'workspace'
SCHEMA = 'portfolio'
TABLE = 'isin_ticker_mapping'

print("=" * 80)
print("STEP 1: Fetching ticker list from Databricks")
print("=" * 80)
print(f"Table: {CATALOG}.{SCHEMA}.{TABLE}")
print(f"Warehouse: {DATABRICKS_WAREHOUSE_ID}")
print(f"Host: {DATABRICKS_HOST}\n")

# Use Databricks SQL Statement Execution API
url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# SQL query to get tickers
sql_query = f"""
SELECT 
    security_isin,
    security_name,
    ticker_symbol
FROM {CATALOG}.{SCHEMA}.{TABLE}
WHERE ticker_symbol IS NOT NULL
ORDER BY security_name
"""

payload = {
    "statement": sql_query,
    "warehouse_id": DATABRICKS_WAREHOUSE_ID,
    "wait_timeout": "30s"
}

try:
    # Execute query
    print(f"Querying Databricks...")
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code != 200:
        print(f"✗ Error: {response.status_code}")
        print(f"Response: {response.text}")
    
    response.raise_for_status()
    result = response.json()
    
    # Check if we need to poll for results
    if result.get('status', {}).get('state') == 'PENDING':
        statement_id = result['statement_id']
        status_url = f"{url}/{statement_id}"
        
        print("Query pending, polling for results...")
        
        # Poll for completion
        for i in range(60):  # 60 attempts, 2 seconds apart = 2 minutes max
            time.sleep(2)
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            result = status_response.json()
            
            state = result.get('status', {}).get('state')
            
            if state == 'SUCCEEDED':
                break
            elif state == 'FAILED':
                error_msg = result.get('status', {}).get('error', {})
                raise Exception(f"Query failed: {error_msg}")
    
    # Extract results
    if result.get('status', {}).get('state') == 'SUCCEEDED':
        manifest = result.get('result', {}).get('data_array', [])
        
        # Parse ticker data
        tickers_data = []
        for row in manifest:
            if len(row) >= 3:
                tickers_data.append({
                    'isin': row[0],
                    'name': row[1],
                    'ticker': row[2]
                })
        
        print(f"✓ Found {len(tickers_data)} tickers\n")
        
        # Print sample
        print("Sample tickers:")
        for ticker in tickers_data[:5]:
            print(f"  {ticker['ticker']}: {ticker['name']}")
        print()
    else:
        raise Exception(f"Unexpected query state: {result.get('status', {}).get('state')}")

except Exception as e:
    print(f"\n✗ Error fetching tickers from Databricks: {str(e)}")
    raise

# STEP 2: Fetch prices with yfinance
print("=" * 80)
print("STEP 2: Fetching stock prices with yfinance")
print("=" * 80 + "\n")

price_data = []

for item in tickers_data:
    isin = item['isin']
    name = item['name']
    ticker = item['ticker']
    
    try:
        # Fetch stock data
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get current price (try multiple fields)
        price = None
        currency = 'USD'
        
        # Try different price fields
        for price_field in ['currentPrice', 'regularMarketPrice', 'previousClose']:
            if price_field in info and info[price_field] is not None:
                price = float(info[price_field])
                break
        
        # Get currency
        if 'currency' in info:
            currency = info['currency']
        
        if price is not None:
            print(f"✓ {name} ({ticker}): {currency} {price:.2f}")
            price_data.append({
                'security_isin': isin,
                'security_name': name,
                'ticker_symbol': ticker,
                'price': price,
                'currency': currency,
                'fetch_timestamp': datetime.now().isoformat()
            })
        else:
            print(f"✗ {name} ({ticker}): No price available")
            price_data.append({
                'security_isin': isin,
                'security_name': name,
                'ticker_symbol': ticker,
                'price': None,
                'currency': None,
                'fetch_timestamp': datetime.now().isoformat()
            })
    
    except Exception as e:
        print(f"⚠ {name} ({ticker}): Error - {str(e)}")
        price_data.append({
            'security_isin': isin,
            'security_name': name,
            'ticker_symbol': ticker,
            'price': None,
            'currency': None,
            'fetch_timestamp': datetime.now().isoformat()
        })

# Create DataFrame
df = pd.DataFrame(price_data)

print(f"\n✓ Completed: {df['price'].notna().sum()}/{len(df)} prices fetched")

# Create prices directory if it doesn't exist
os.makedirs('prices', exist_ok=True)

# Save to CSV
output_file = 'prices/latest_prices.csv'
df.to_csv(output_file, index=False)

print(f"✓ Saved prices to {output_file}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total securities: {len(df)}")
print(f"Prices fetched: {df['price'].notna().sum()}")
print(f"Failed/unavailable: {df['price'].isna().sum()}")
print("=" * 80)
