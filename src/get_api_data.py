import requests
import pandas as pd
import os
import json
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

TIBER_TOKEN = os.environ.get("TIBER_TOKEN")
TIBBER_API_URL = os.environ.get("TIBBER_API_URL")
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _make_graphql_request(query, variables=None):
    """
    Helper function to send a GraphQL request to the Tibber API.
    """
    if TIBER_TOKEN is None:
        raise ValueError("TIBBER_TOKEN environment variable not set.")

    headers = {
        "Authorization": f"Bearer {TIBER_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"query": query, "variables": variables}

    try:
        response = requests.post(TIBBER_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return None

def get_home_id():
    """
    Fetches the home ID associated with the Tibber account.
    """
    query = """
    {
      viewer {
        homes {
          id
          address {
            address1
            postalCode
            city
            country
          }
        }
      }
    }
    """
    data = _make_graphql_request(query)
    if data and 'data' in data and 'viewer' in data['data'] and 'homes' in data['data']['viewer']:
        homes = data['data']['viewer']['homes']
        if homes:

            print(f"Found home: {homes[0]['address']['address1']}, {homes[0]['address']['city']} (ID: {homes[0]['id']})")
            return homes[0]['id'], homes[0]['address']['address1']
    print("Could not retrieve home ID. Make sure your token is valid and linked to a home.")
    return None

def get_current_and_upcoming_prices(home_id):
    """
    Fetches current day and tomorrow's energy prices for a given home ID.
    Tomorrow's prices are usually available after 12:00 CET/CEST.
    """
    query = """
    query ($homeId: ID!) {
      viewer {
        home(id: $homeId) {
          currentSubscription {
            priceInfo {
              current {
                total
                startsAt
                level
              }
              today {
                total
                startsAt
                level
              }
              tomorrow {
                total
                startsAt
                level
              }
            }
          }
        }
      }
    }
    """
    variables = {"homeId": home_id}
    data = _make_graphql_request(query, variables)

    prices = []
    if data and 'data' in data and 'viewer' in data['data'] and 'home' in data['data']['viewer']['home']:
        price_info = data['data']['viewer']['home']['currentSubscription']['priceInfo']

        # Current price
        if price_info.get('current'):
            prices.append(price_info['current'])

        # Today's prices
        if price_info.get('today'):
            prices.extend(price_info['today'])

        # Tomorrow's prices
        if price_info.get('tomorrow'):
            prices.extend(price_info['tomorrow'])

    df = pd.DataFrame(prices)
    if not df.empty:
        df['startsAt'] = pd.to_datetime(df['startsAt'])
        df = df.drop_duplicates(subset='startsAt').sort_values('startsAt').reset_index(drop=True)
        print(f"Successfully retrieved {len(df)} price entries.")
    else:
        print("No current or upcoming price data found.")
    return df

def get_historical_consumption(home_id, num_hours=720): # 720 hours = 30 days
    """
    Fetches historical hourly consumption data for a given home ID.
    The 'last' argument takes the number of hours.
    """
    query = """
    query ($homeId: ID!, $last: Int!) {
      viewer {
        home(id: $homeId) {
          consumption(resolution: HOURLY, last: $last) {
            nodes {
              from
              to
              consumption # kWh consumed
              unitPrice   # Price ex. VAT
              unitPriceVAT # VAT component
              totalCost   # consumption * unitPriceVAT (including VAT)
              currency
            }
          }
        }
      }
    }
    """
    variables = {"homeId": home_id, "last": num_hours}
    data = _make_graphql_request(query, variables)

    consumption_data = []
    if data and 'data' in data and 'viewer' in data['data'] and 'home' in data['data']['viewer']['home']:
        if data['data']['viewer']['home'].get('consumption') and data['data']['viewer']['home']['consumption'].get('nodes'):
            consumption_data = data['data']['viewer']['home']['consumption']['nodes']

    df = pd.DataFrame(consumption_data)
    if not df.empty:
        df['from'] = pd.to_datetime(df['from'])
        df['to'] = pd.to_datetime(df['to'])
        df['totalPrice'] = df['unitPrice'] + df['unitPriceVAT'] # Reconstruct total price
        df = df.sort_values('from').reset_index(drop=True)
        print(f"Successfully retrieved {len(df)} historical consumption entries.")
    else:
        print(f"No historical consumption data found for the last {num_hours} hours.")
    return df

def get_live_measurement(home_id):
    """
    Fetches the current live measurement data from Tibber Pulse.
    Requires a Tibber Pulse device.
    """

    query = """
    subscription ($homeId: ID!) {
      liveMeasurement(homeId: $homeId) {
        timestamp
        power             # Current power consumption in Watts (W)
        accumulatedConsumption # Accumulated consumption since midnight in kWh
        accumulatedCost   # Accumulated cost since midnight
        currency
        minPower
        averagePower
        maxPower
        powerProduction   # Net power production in Watts (W) if you have solar/etc.
      }
    }
    """
    variables = {"homeId": home_id}

    print("Live measurement data is best retrieved via WebSocket subscriptions for real-time updates.")
    data = _make_graphql_request(query, variables)
    return data

if __name__ == "__main__":

    print("--- Fetching Home ID ---")
    my_home_id, my_home_address = get_home_id()

    if my_home_id:
        print(f"\n--- Fetching Current & Upcoming Prices for {my_home_address} ---")
        price_df = get_current_and_upcoming_prices(my_home_id)
        if not price_df.empty:
            print(price_df.head())
            print(f"Price data from {price_df['startsAt'].min()} to {price_df['startsAt'].max()}")
            price_df.to_csv(os.path.join("..", "data/raw", "historical_consumption.csv"))

        print(f"\n--- Fetching Historical Consumption for {my_home_id} (last 720 hours = 30 days) ---")
        consumption_df = get_historical_consumption(my_home_id, num_hours=720)
        if not consumption_df.empty:
            print(consumption_df.head())
            print(f"Consumption data from {consumption_df['from'].min()} to {consumption_df['to'].max()}")
            consumption_df.to_csv(os.path.join("..", "data/raw", "historical_consumption.csv"))

        print(f"\n--- Attempting Live Measurement for {my_home_id} (single snapshot) ---")
        live_data = get_live_measurement(my_home_id)
        if live_data:
            print(json.dumps(live_data, indent=2))
    else:
        print("Exiting as home ID could not be retrieved.")
