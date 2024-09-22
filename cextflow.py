import requests
import csv
from datetime import datetime
from io import StringIO
import json
import time

def fetch_volume_data(token, days=30):
    url = f"https://api.spotonchain.com/api/smart_trader/get_cex_volume?token={token}&days={days}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data['result'] != "success":
            raise ValueError(f"API request for {token} was not successful")
        
        volume_data = data['data']['volume_data']
        
        formatted_data = [
            {
                "token": token,
                "date": datetime.fromtimestamp(entry[0]).strftime('%Y-%m-%d'),
                "volume": entry[1]
            }
            for entry in volume_data
        ]
        
        return formatted_data
    
    except requests.RequestException as e:
        print(f"Error fetching data for {token}: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error processing data for {token}: {e}")
        return None

def data_to_csv(data):
    if not data:
        return None
    
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=["token", "date", "volume"])
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()

def upload_to_dune(csv_data, dune_api_key):
    dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"
    payload = json.dumps({
        "data": csv_data,
        "description": "Token Volume Data",
        "table_name": "token_volume_data",
        "is_private": False
    })
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': dune_api_key
    }
    response = requests.post(dune_upload_url, headers=headers, data=payload)
    return response.text

def main():
    tokens = ["ETH", "SOL", "TON", "LINK", "UNI", "PEPE", "POL", "IMX", "AAVE", "RNDR", "INJ", "MKR", "LDO", "ONDO", "BEAM", "STRK", "PENDLE", "CAKE", "ZRO", "ZK", "MOG", "AXL"]
    dune_api_key = "p0RZJpTPCUn9Cn7UTXEWDhalc53QzZXV"  # Replace with your actual Dune API key
    
    all_data = []
    
    for token in tokens:
        print(f"Fetching data for {token}...")
        volume_data = fetch_volume_data(token)
        if volume_data:
            all_data.extend(volume_data)
        time.sleep(2)  # 2-second delay between API calls
    
    if all_data:
        csv_output = data_to_csv(all_data)
        if csv_output:
            print("Uploading data to Dune...")
            upload_result = upload_to_dune(csv_output, dune_api_key)
            print(f"Upload result: {upload_result}")
        else:
            print("Failed to convert data to CSV.")
    else:
        print("No data to upload.")

if __name__ == "__main__":
    main()
