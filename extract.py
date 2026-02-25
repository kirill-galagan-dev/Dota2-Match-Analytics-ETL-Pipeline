import requests
from datetime import datetime

PLAYER_ID = 62576805
API_URL = f"https://api.opendota.com/api/players/{PLAYER_ID}/matches"

def fetch_data():
    print(f"Requesting player data {PLAYER_ID}...")
    params = {'limit': 10}
    response = requests.get(API_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        if not data:
            return None

        first_match = data[0]

        # 1. Determine the player side
        # player_slot < 128 means Radiant
        is_radiant = first_match['player_slot'] < 128

        # 2. Getting the Radiant victory flag from the API
        radiant_win = first_match['radiant_win']

        # 3. We determine the player's final result
        # Win if (was for Radiant and Radiant won) OR (was for Dire and Radiant lost)
        player_won = (is_radiant == radiant_win)

        readable_date = datetime.fromtimestamp(first_match['start_time'])

        print(f"\nMatch details {first_match['match_id']}:")
        print(f"- Side: {'Radiant' if is_radiant else 'Dire'}")
        print(f"- Date: {readable_date}")
        print(f"- Result: {'VICTORY' if player_won else 'DEFEAT'}")

        return data
    return None

if __name__ == "__main__":
    fetch_data()