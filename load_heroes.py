import requests
import psycopg2
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Database connection parameters
DB_CONFIG = {
    "dbname": "dota_etl",
    "user": "your_username", # Replace with your actual username
    "password": "your_password", # Replace with your actual password
    "host": "localhost",
    "port": "5432"
}

def load_heroes_dimension():

    url = "https://api.opendota.com/api/heroes"

    # --- Phase 1: Extract ---
    try:
        logging.info("Starting extraction: Fetching data from OpenDota API...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        heroes = response.json()
        logging.info(f"Extraction successful: Retrieved {len(heroes)} heroes.")
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return

    # --- Phase 2: Load ---
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # SQL Query: UPSERT logic to handle existing records
        # EXCLUDED refers to the row that was proposed for insertion
        upsert_query = """
            INSERT INTO heroes (hero_id, name, localized_name, primary_attr)
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (hero_id) DO UPDATE SET
                name = EXCLUDED.name,
                localized_name = EXCLUDED.localized_name,
                primary_attr = EXCLUDED.primary_attr;
        """

        # Prepare data for batch insertion (List of tuples)
        data_to_insert = [
            (h['id'], h['name'], h['localized_name'], h['primary_attr'])
            for h in heroes
        ]

        # Execute batch insert for high performance
        cur.executemany(upsert_query, data_to_insert)

        # Commit transaction to make changes permanent
        conn.commit()
        logging.info("Load successful: Hero data persisted to PostgreSQL.")

    except Exception as e:
        logging.error(f"Database operation failed: {e}")
        if conn:
            conn.rollback()
    finally:
        # Ensure the connection is closed even if an error occurs
        if conn:
            cur.close()
            conn.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    load_heroes_dimension()


