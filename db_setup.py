import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def init_db(db_path="revbox_data.db"):
    """Initializes the SQLite database with strict schemas."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Raw Ingestion Log (Separates raw vs processed)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_ingestion_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                carrier_name TEXT,
                raw_row_json TEXT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 2. Config-Driven Mapping
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mapping_config (
                carrier_name TEXT,
                raw_column_name TEXT,
                mapped_schema_column TEXT,
                PRIMARY KEY (carrier_name, raw_column_name)
            )
        ''')

        # 3 & 4. Lookup Tables for Validation
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ref_status (
                original_status TEXT PRIMARY KEY,
                normalized_status TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ref_category (
                original_category TEXT PRIMARY KEY,
                normalized_category TEXT
            )
        ''')

        # 5. The Final Normalized Output
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS normalized_policies (
                policy_id TEXT PRIMARY KEY,
                customer_name TEXT,
                policy_type TEXT,
                premium REAL,
                effective_date DATE,
                status TEXT,
                carrier_name TEXT
            )
        ''')

        conn.commit()
        logging.info(f"Database initialized successfully at {db_path}")
        
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    init_db()