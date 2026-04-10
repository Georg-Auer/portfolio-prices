#!/usr/bin/env python3
"""
Fetch stock prices using yfinance
Reads tickers from tickers.json (created by fetch_tickers.py)
Saves prices to prices/latest_prices.csv
"""

import json
import pandas as pd
import yfinance as yf
from datetime import datetime
import os

print("Fetching stock prices with yfinance...\n")

# Load tickers from JSON
with open('tickers.json', 'r') as f:
    tickers_data = json.load(f)

print(f"Loaded {len(tickers_data)} tickers\n")

# Fetch prices
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
            print(f"  ✓ {name} ({ticker}): {currency} {price:.2f}")
            price_data.append({
                'security_isin': isin,
                'security_name': name,
                'ticker_symbol': ticker,
                'price': price,
                'currency': currency,
                'fetch_timestamp': datetime.now().isoformat()
            })
        else:
            print(f"  ✗ {name} ({ticker}): No price available")
            price_data.append({
                'security_isin': isin,
                'security_name': name,
                'ticker_symbol': ticker,
                'price': None,
                'currency': None,
                'fetch_timestamp': datetime.now().isoformat()
            })
    
    except Exception as e:
        print(f"  ⚠ {name} ({ticker}): Error - {str(e)}")
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
print(f"\nSample data:")
print(df.head())