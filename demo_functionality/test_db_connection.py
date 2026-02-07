"""
Test database connection and check for data.
"""
from db_config import get_default_connection

def test_connection():
    """Test connection and query data."""
    print("Testing database connection...")

    try:
        db_conn = get_default_connection()
        with db_conn as conn:
            cursor = conn.cursor()

            # Test Assets table
            print("\n--- Testing Assets table ---")
            cursor.execute("SELECT COUNT(*) as count FROM dbo.Assets")
            count = cursor.fetchone()[0]
            print(f"Total assets in database: {count}")

            if count > 0:
                cursor.execute("SELECT TOP 3 SegmentID, Name, MAOP_PSIG FROM dbo.Assets")
                print("Sample assets:")
                for row in cursor.fetchall():
                    print(f"  {row.SegmentID}: {row.Name} (MAOP: {row.MAOP_PSIG})")

            # Test Readings table
            print("\n--- Testing Readings table ---")
            cursor.execute("SELECT COUNT(*) as count FROM dbo.Readings")
            count = cursor.fetchone()[0]
            print(f"Total readings in database: {count}")

            if count > 0:
                cursor.execute("SELECT TOP 3 Timestamp, SegmentID, PressurePSIG, DataQuality FROM dbo.Readings")
                print("Sample readings:")
                for row in cursor.fetchall():
                    print(f"  {row.Timestamp} - {row.SegmentID}: {row.PressurePSIG} PSIG (Quality: {row.DataQuality})")

            # Test GOOD quality readings
            cursor.execute("SELECT COUNT(*) as count FROM dbo.Readings WHERE DataQuality = 'GOOD'")
            good_count = cursor.fetchone()[0]
            print(f"\nReadings with GOOD quality: {good_count}")

        print("\n✓ Database connection successful!")

    except Exception as e:
        print(f"\n✗ Error connecting to database: {e}")
        print(f"  Error type: {type(e).__name__}")

if __name__ == "__main__":
    test_connection()
