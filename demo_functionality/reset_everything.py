"""
Nuclear option: Reset everything and start fresh.
Run this when things are messed up.
"""
from db_config import get_default_connection

def reset_everything():
    """Delete EVERYTHING and start fresh."""
    print("\n" + "=" * 80)
    print("NUCLEAR RESET - Deleting ALL data from database")
    print("=" * 80 + "\n")

    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        # Delete in proper order (respecting foreign keys)
        tables = [
            "Compliance",
            "Readings",  # This will delete ALL readings, including old ones
            "PressureTestRecords",
            "Sensors",
            "Assets",
            "Users"
        ]

        for table in tables:
            try:
                cursor.execute(f"DELETE FROM dbo.{table}")
                deleted = cursor.rowcount
                print(f"  ✓ Deleted {deleted} rows from {table}")
            except Exception as e:
                print(f"  ⚠ {table}: {str(e)[:100]}")

        conn.commit()

    print("\n" + "=" * 80)
    print("✓ DATABASE COMPLETELY CLEARED")
    print("=" * 80)
    print("\nNext step: Run populate_demo_data.py")
    print("  cd demo_functionality")
    print("  python populate_demo_data.py")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    response = input("⚠️  This will DELETE ALL DATA. Are you sure? (yes/no): ")

    if response.lower() == 'yes':
        reset_everything()
    else:
        print("Cancelled.")
