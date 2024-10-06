import requests
import csv
from datetime import datetime
import json
from io import StringIO
import time

def fetch_data(coin_id, days=180, api_key=None):
    url = f"https://pro-api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "daily"
    }
    headers = {"X-Cg-Pro-Api-Key": api_key} if api_key else {}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Error fetching data for {coin_id}:")
        print(f"URL: {err.request.url}")
        print(f"Method: {err.request.method}")
        print(f"Headers: {err.request.headers}")
        if err.response is not None:
            print(f"Status code: {err.response.status_code}")
            print("Response headers:")
            for key, value in err.response.headers.items():
                print(f"  {key}: {value}")
            print("Response content:")
            try:
                content = json.loads(err.response.content)
                print(json.dumps(content, indent=2))
            except json.JSONDecodeError:
                print(err.response.content)
        else:
            print("No response received")
    return None

def calculate_changes(prices, volumes, days):
    if len(prices) < days:
        return None, None
    
    start_price = prices[-days][1]
    end_price = prices[-1][1]
    price_change = (end_price - start_price) / start_price * 100
    
    start_volume = sum(volume for _, volume in volumes[-2*days:-days])
    end_volume = sum(volume for _, volume in volumes[-days:])
    volume_change = (end_volume - start_volume) / start_volume * 100
    
    return price_change, volume_change

def convert_to_csv(headers, rows):
    csv_file = StringIO()
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(headers)
    csv_writer.writerows(rows)
    csv_data = csv_file.getvalue()
    csv_file.close()
    return csv_data

def upload_to_dune(csv_data, dune_api_key):
    dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"
    payload = json.dumps({
        "data": csv_data,
        "description": "Crypto Market Data",
        "table_name": "crypto_market_data",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': dune_api_key
    }
    try:
        response = requests.post(dune_upload_url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Error uploading to Dune: {err}")
        return None

def main():
    coingecko_api_key = "CG-ep7WBgRCGeDria6EeL1jkPot"  # Replace with your actual CoinGecko Pro API key
    dune_api_key = "YOUR_DUNE_API_KEY"  # Replace with your actual Dune API key

    coins = {
        'ETH': 'ethereum', 'SOL': 'solana', 'TON': 'the-open-network', 'ADA': 'cardano',
        'DOGE': 'dogecoin', 'AVAX': 'avalanche-2', 'LINK': 'chainlink', 'DOT': 'polkadot',
        'UNI': 'uniswap', 'NEAR': 'near', 'SUI': 'sui', 'PEPE': 'pepe',
        'TAO': 'bittensor', 'ICP': 'internet-computer', 'POL': 'polygon-ecosystem-token',
        'IMX': 'immutable-x', 'AAVE': 'aave', 'RNDR': 'render-token', 'FIL': 'filecoin',
        'INJ': 'injective-protocol', 'OP': 'optimism', 'MKR': 'maker', 'AR': 'arweave',
        'SEI': 'sei-network', 'TIA': 'celestia', 'JUP': 'jupiter-exchange-solana',
        'LDO': 'lido-dao', 'ONDO': 'ondo-finance', 'POPCAT': 'popcat', 'BEAM': 'beam-2',
        'CKB': 'nervos-network', 'FLR': 'flare-networks', 'STRK': 'starknet',
        'PENDLE': 'pendle', 'AERO': 'aerodrome-finance', 'CAKE': 'pancakeswap-token',
        'ZRO': 'layerzero', 'ZK': 'zksync', 'MOG': 'mog-coin', 'AXL': 'axelar'
    }
    
    periods = [7, 30, 60, 90]
    headers = ['date', 'symbol', 'coin_id', 'volume_change_7d', 'price_change_7d', 'volume_change_30d', 'price_change_30d',
               'volume_change_60d', 'price_change_60d', 'volume_change_90d', 'price_change_90d']
    
    all_rows = []
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    for symbol, coin_id in coins.items():
        print(f"Fetching data for {symbol} ({coin_id})...")
        data = fetch_data(coin_id, api_key=coingecko_api_key)
        if data is None:
            print(f"Skipping {symbol} ({coin_id}) due to fetch failure.")
            continue

        prices = data['prices'][:-1]  # Exclude the last data point
        volumes = data['total_volumes'][:-1]  # Exclude the last data point
        
        row = [current_date, symbol, coin_id]
        
        for period in periods:
            price_change, volume_change = calculate_changes(prices, volumes, period)
            if price_change is not None and volume_change is not None:
                row.extend([f"{volume_change:.2f}", f"{price_change:.2f}"])
            else:
                row.extend(["N/A", "N/A"])
        
        all_rows.append(row)
        
        # Small delay between requests
        time.sleep(0.1)
    
    # Convert results to CSV
    csv_data = convert_to_csv(headers, all_rows)
    
    # Upload to Dune
    print("Uploading data to Dune...")
    upload_result = upload_to_dune(csv_data, dune_api_key)
    if upload_result:
        print("Data successfully uploaded to Dune.")
    else:
        print("Failed to upload data to Dune.")
    
    # Write to local CSV file
    with open('crypto_market_data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(all_rows)
    
    print("Data has been written to crypto_market_data.csv")

if __name__ == "__main__":
    main()
