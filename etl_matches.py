import requests
import pandas as pd
import psycopg2
from psycopg2 import extras
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Configuration
PLAYER_ID = 62576805
DB_CONFIG = {
    "dbname": "dota_etl",
    "user": "your_username", # Replace with your actual username
    "password": "your_password", # Replace with your actual password
    "host": "localhost",
    "port": "5432"
}


def run_matches_etl():
    url = f"https://api.opendota.com/api/players/{PLAYER_ID}/matches?limit=20"

    # --- PHASE 1: EXTRACT ---
    try:
        logging.info(f"Extracting match history for Player {PLAYER_ID}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Load into Pandas DataFrame
        df = pd.DataFrame(response.json())
        logging.info(f"Extracted {len(df)} matches with detailed stats.")
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return

    # --- PHASE 2: TRANSFORM ---
    logging.info("Transforming data with Pandas...")

    # Calculate Side and Result
    df['side'] = df['player_slot'].apply(lambda x: 'Radiant' if x < 128 else 'Dire')
    df['is_win'] = df.apply(
        lambda r: r['radiant_win'] if r['side'] == 'Radiant' else not r['radiant_win'],
        axis=1
    )

    # Convert timestamps and add metadata
    df['start_time'] = pd.to_datetime(df['start_time'], unit='s')
    df['player_id'] = PLAYER_ID

    # Check for last_hits (defensive check)
    if 'last_hits' not in df.columns:
        df['last_hits'] = 0

    # Split into our Star Schema tables
    matches_df = df[['match_id', 'start_time', 'duration', 'radiant_win', 'game_mode']]

    # We now include 'side' and 'is_win' in our player data
    players_performance_df = df[[
        'match_id', 'player_id', 'hero_id', 'kills', 'deaths', 'assists', 'last_hits', 'side', 'is_win'
    ]]

    # --- PHASE 3: LOAD ---
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Load Matches
        logging.info("Loading into 'matches' table...")
        match_query = """
                      INSERT INTO matches (match_id, start_time, duration, radiant_win, game_mode)
                      VALUES %s ON CONFLICT (match_id) DO NOTHING; \
                      """
        match_records = [tuple(x) for x in matches_df.to_numpy().tolist()]
        extras.execute_values(cur, match_query, match_records)

        # Load Player Metrics with side and win status
        logging.info("Loading into 'match_players' table...")
        player_query = """
                       INSERT INTO match_players (match_id, player_id, hero_id, kills, deaths, assists, last_hits, side, \
                                                  is_win)
                       VALUES %s ON CONFLICT (match_id, player_id) DO \
                       UPDATE \
                       SET
                           side = EXCLUDED.side, is_win = EXCLUDED.is_win; \
                       """
        player_records = [tuple(x) for x in players_performance_df.to_numpy().tolist()]
        extras.execute_values(cur, player_query, player_records)

        conn.commit()
        logging.info("ETL Job completed successfully!")

    except Exception as e:
        logging.error(f"Database Load failed: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    run_matches_etl()
