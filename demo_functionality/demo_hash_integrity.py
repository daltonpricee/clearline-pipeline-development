"""
Demo script to show the Immutable Hash Chain in action.

This demonstrates:
1. Inserting readings with hash chain
2. Verifying the chain integrity
3. Detecting tampering when data is manually changed
"""
from datetime import datetime, timedelta
from hash_chain import (
    insert_reading_with_hash,
    verify_hash_chain,
    rebuild_hash_chain
)
from db_config import get_default_connection


def demo_insert_readings():
    """Demo: Insert sample readings with hash chain."""
    print("=" * 80)
    print("DEMO: Inserting Readings with Hash Chain")
    print("=" * 80)

    # Sample data for insertion
    base_time = datetime(2026, 2, 7, 10, 0, 0)

    readings = [
        ("SEG-01", 1, 750.0, 1000.0),
        ("SEG-02", 2, 800.0, 950.0),
        ("SEG-03", 3, 650.0, 875.0),
        ("SEG-01", 1, 755.0, 1000.0),  # 5 minutes later
        ("SEG-02", 2, 820.0, 950.0),   # Pressure increasing
        ("SEG-02", 2, 855.0, 950.0),   # 90% MAOP - WARNING level
    ]

    for i, (segment_id, sensor_id, pressure, maop) in enumerate(readings):
        timestamp = base_time + timedelta(minutes=i * 5)

        reading_id, hash_sig = insert_reading_with_hash(
            timestamp=timestamp,
            segment_id=segment_id,
            sensor_id=sensor_id,
            pressure_psig=pressure,
            maop_psig=maop,
            recorded_by="SCADA",
            data_source="SCADA",
            data_quality="GOOD"
        )

        print(f"✓ ReadingID {reading_id}: {segment_id} @ {timestamp}")
        print(f"  Pressure: {pressure} PSIG | Hash: {hash_sig[:16]}...")

    print(f"\n✓ Inserted {len(readings)} readings with hash chain")


def demo_verify_integrity():
    """Demo: Verify the hash chain integrity."""
    print("\n" + "=" * 80)
    print("DEMO: Verifying Hash Chain Integrity")
    print("=" * 80)

    is_valid, broken_at, total = verify_hash_chain(verbose=True)

    print("\n" + "-" * 80)
    if is_valid:
        print(f"✓ HASH CHAIN VALID - All {total} readings verified")
        print("  Data integrity confirmed - No tampering detected")
    else:
        print(f"❌ HASH CHAIN BROKEN at ReadingID {broken_at}")
        print(f"  {total} readings checked before break")
        print("  FORENSIC ALERT: Data has been tampered with!")


def demo_tamper_detection():
    """Demo: Show what happens when data is tampered with."""
    print("\n" + "=" * 80)
    print("DEMO: Tampering Detection")
    print("=" * 80)

    # First, verify the chain is valid
    print("Step 1: Verify chain is currently valid...")
    is_valid, _, _ = verify_hash_chain()

    if not is_valid:
        print("  Chain is already broken. Run rebuild_hash_chain() first.")
        return

    print("  ✓ Chain is valid\n")

    # Now manually tamper with a record
    print("Step 2: Simulating tampering...")
    print("  Manually changing a pressure reading from 820 to 900 PSIG...")

    db_conn = get_default_connection()
    with db_conn as conn:
        cursor = conn.cursor()

        # Find a reading to tamper with
        cursor.execute("""
            SELECT TOP 1 ReadingID, SegmentID, PressurePSIG
            FROM dbo.Readings
            WHERE PressurePSIG = 820.0
        """)
        result = cursor.fetchone()

        if result:
            reading_id = result.ReadingID
            print(f"  Found ReadingID {reading_id} with pressure {result.PressurePSIG}")

            # Tamper with it (change pressure without updating hash)
            cursor.execute("""
                UPDATE dbo.Readings
                SET PressurePSIG = 900.0
                WHERE ReadingID = ?
            """, (reading_id,))
            conn.commit()

            print(f"  ✓ Changed pressure to 900.0 PSIG (hash NOT updated)\n")

            # Now verify the chain
            print("Step 3: Re-verify hash chain...")
            is_valid, broken_at, total = verify_hash_chain(verbose=False)

            if not is_valid:
                print(f"  ❌ TAMPERING DETECTED at ReadingID {broken_at}!")
                print(f"  The hash chain is broken - data has been modified!")
                print(f"  This provides FORENSIC PROOF of data alteration.")

                # Restore the original value
                print("\nStep 4: Restoring original value...")
                cursor.execute("""
                    UPDATE dbo.Readings
                    SET PressurePSIG = 820.0
                    WHERE ReadingID = ?
                """, (reading_id,))
                conn.commit()
                print("  ✓ Restored to 820.0 PSIG")

                # Rebuild the chain
                print("\nStep 5: Rebuilding hash chain...")
                count = rebuild_hash_chain()
                print(f"  ✓ Rebuilt hash chain for {count} readings")
        else:
            print("  No suitable reading found for tampering demo")


def main():
    """Run all demos."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "ClearLine Pipeline Hash Chain Demo" + " " * 24 + "║")
    print("║" + " " * 20 + "Immutable Forensic Data Integrity" + " " * 25 + "║")
    print("╚" + "=" * 78 + "╝")

    try:
        # Check if we have any readings
        db_conn = get_default_connection()
        with db_conn as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.Readings")
            count = cursor.fetchone()[0]

        if count == 0:
            print("\n⚠️  Database is empty!")
            print("Please run: python populate_demo_data.py first\n")
            return
        else:
            print(f"\n✓ Found {count} readings in database with hash chain")
            print("  (Use populate_demo_data.py to reset demo data)\n")

        # Always run verification
        demo_verify_integrity()

        # Run tampering demo automatically for partners
        print("\n" + "=" * 80)
        print("THE FORENSIC PROOF DEMO - Tampering Detection")
        print("=" * 80)
        response = input("\nReady to demonstrate tampering detection? (y/n): ")
        if response.lower() == 'y':
            demo_tamper_detection()

            # Verify again after restoration
            print("\nFinal verification after restoration:")
            demo_verify_integrity()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
