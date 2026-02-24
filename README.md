# Dota2-Match-Analytics-ETL-Pipeline

## Project Overview:
A modular End-to-End ETL (Extract, Transform, Load) pipeline that pulls personal Dota 2 match statistics from the **OpenDota API**, transforms the data using **Pandas**, and stores it in a **PostgreSQL** database using a Star Schema

## Tech Stack
* **Language:** Python 3.12
* **Data Processing:** Pandas
* **Database:** PostgreSQL
* **API:** OpenDota API
* **Database Driver:** Psycopg2
* 
## Database Architecture (Star Schema)
* **Fact Tables:** `matches`, `match_players`
* **Dimension Table:** `heroes`

## Key Features
* **Automated Extraction:** Fetches match history and hero dimensions.
* **Data Cleaning:** Handles Unix timestamps, data types, and UTF-8 encoding.
* **Feature Engineering:** Calculates match results (Win/Loss) and player side (Radiant/Dire) in Python.
* **Idempotency:** Uses `UPSERT` logic (`ON CONFLICT`) to prevent duplicate data.

## Current Status
* Database schema is fully implemented.
* Hero and Match ETL scripts are functional.
* Orchestration (`main.py`) is under final development.
