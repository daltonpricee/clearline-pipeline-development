"""
One-click demo setup:
  1. Creates the 'Clearline' PostgreSQL database if it doesn't exist
  2. Runs create_database.sql to create all tables, indexes, and triggers
  3. Populates with fresh demo data

Run from the demo_functionality directory:
    python setup_demo.py
"""
import os
import sys
import psycopg2
import psycopg2.extras

# ── Connection settings ────────────────────────────────────────────────────────
# Change these to match your PostgreSQL setup.
PG_HOST     = "localhost"
PG_PORT     = 5432
PG_USER     = "postgres"
PG_PASSWORD = "#Huskies2016"          # Set to your password string if you have one, e.g. "mypassword"
DB_NAME     = "Clearline"
# ──────────────────────────────────────────────────────────────────────────────


def create_database_if_missing():
    """
    Connect to the default 'postgres' database and create 'Clearline' if it
    doesn't already exist.  CREATE DATABASE must run outside a transaction so
    we use autocommit=True.
    """
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname="postgres",
        user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
    exists = cursor.fetchone()

    if exists:
        print(f"  Database '{DB_NAME}' already exists — skipping creation.")
    else:
        cursor.execute(f'CREATE DATABASE "{DB_NAME}"')
        print(f"  Database '{DB_NAME}' created.")

    cursor.close()
    conn.close()


def run_schema_sql():
    """
    Execute create_database.sql against the Clearline database to create
    all tables, indexes, and triggers.
    """
    sql_path = os.path.join(os.path.dirname(__file__), "create_database.sql")

    with open(sql_path, "r") as f:
        schema_sql = f.read()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=DB_NAME,
        user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True          # DDL statements don't need an explicit commit
    cursor = conn.cursor()
    cursor.execute(schema_sql)
    cursor.close()
    conn.close()
    print("  Schema applied (tables, indexes, triggers ready).")


def populate():
    """Import and run all populate functions."""
    from populate_demo_data import (
        populate_users,
        populate_assets,
        populate_sensors,
        populate_readings_with_story,
        populate_operator_acknowledgment,
    )

    populate_users()
    populate_assets()
    populate_sensors()
    populate_readings_with_story()
    populate_operator_acknowledgment()


def setup_demo():
    """Full setup in one command."""
    print("\n" + "=" * 80)
    print(" " * 20 + "ClearLine Demo - Complete Setup")
    print("=" * 80 + "\n")

    print("Step 1: Creating database...")
    create_database_if_missing()

    print("\nStep 2: Creating tables, indexes, and triggers...")
    run_schema_sql()

    print("\nStep 3: Populating demo data...")
    populate()

    print("\n" + "=" * 80)
    print("✓ DEMO SETUP COMPLETE")
    print("=" * 80)
    print("\nDatabase Summary:")
    print("  • 4 pipeline segments (assets)")
    print("  • 4 pressure sensors")
    print("  • 3 users (operators/engineers)")
    print("  • 36 pressure readings with cryptographic hash chain")
    print("  • 1 operator acknowledgment (audit trail)")
    print("\nDrift Story Timeline (with Transient Filter Demo):")
    print("  10:00 - All segments normal")
    print("  10:02 - SEG-02 crosses 90% MAOP → WARNING")
    print("  10:03 - SEG-01 TRANSIENT SPIKE (96%) → FILTERED (not flagged)")
    print("  10:04 - SEG-01 returns to normal (proves spike was transient)")
    print("  10:07 - SEG-02 crosses 95% MAOP → CRITICAL (SUSTAINED)")
    print("  10:08 - Operator acknowledges")
    print("  10:09 - SEG-04 TRANSIENT SPIKE (97%) → FILTERED (not flagged)")
    print("  10:10 - SEG-04 returns to normal (proves spike was transient)")
    print("  10:12 - SEG-02 crosses 100% MAOP → VIOLATION (SUSTAINED)")
    print("\n  ClearLine's Smart Filter:")
    print("     - 2 transient spikes FILTERED (no nuisance alarms)")
    print("     - 1 sustained drift FLAGGED (real issue detected)")
    print("\nRun the dashboard:")
    print("  streamlit run dashboard.py")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        setup_demo()
    except psycopg2.OperationalError as e:
        print(f"\n❌ Could not connect to PostgreSQL: {e}")
        print("\nCheck that:")
        print(f"  • PostgreSQL is running on {PG_HOST}:{PG_PORT}")
        print(f"  • Username '{PG_USER}' is correct")
        print(f"  • PG_PASSWORD at the top of this file is set correctly")
        sys.exit(1)
    except Exception as e:
        import traceback
        print(f"\n❌ Setup failed: {e}")
        traceback.print_exc()
        sys.exit(1)
