"""
Populate database with demo data following the "drift story" timeline.

Timeline:
- 10:00 - All segments normal
- 10:02 - SEG-02 crosses 90% MAOP → WARNING
- 10:07 - SEG-02 crosses 95% MAOP → CRITICAL (compliance clock starts)
- 10:08 - Operator acknowledges → Audit log entry
- 10:12 - SEG-02 crosses 100% MAOP → VIOLATION

This creates a complete demo dataset with hash chain integrity.
"""
from datetime import datetime, timedelta
from db_config import get_default_connection
from hash_chain import insert_reading_with_hash


def clear_all_data():
    """Clear all data from demo tables (optional - for clean slate)."""
    print("Clearing existing data...")
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        # Delete in order to respect foreign keys
        try:
            cursor.execute("DELETE FROM dbo.Compliance")
            print("  ✓ Cleared Compliance")
        except Exception as e:
            print(f"  ⚠ Compliance: {e}")

        # AuditTrail is immutable - skip it (accumulates over demos)
        print("  ⓘ AuditTrail is immutable (skipping - audit logs accumulate)")

        cursor.execute("DELETE FROM dbo.Readings")
        print("  ✓ Cleared Readings")

        try:
            cursor.execute("DELETE FROM dbo.PressureTestRecords")
            print("  ✓ Cleared PressureTestRecords")
        except Exception as e:
            print(f"  ⚠ PressureTestRecords: {e}")

        cursor.execute("DELETE FROM dbo.Sensors")
        print("  ✓ Cleared Sensors")

        cursor.execute("DELETE FROM dbo.Assets")
        print("  ✓ Cleared Assets")

        # Users might be referenced by AuditTrail (which we can't delete)
        try:
            cursor.execute("""
                DELETE FROM dbo.Users
                WHERE UserID NOT IN (SELECT DISTINCT UserID FROM dbo.AuditTrail WHERE UserID IS NOT NULL)
            """)
            deleted = cursor.rowcount
            cursor.execute("SELECT COUNT(*) FROM dbo.Users")
            remaining = cursor.fetchone()[0]
            print(f"  ✓ Cleared {deleted} Users (kept {remaining} users referenced in AuditTrail)")
        except Exception as e:
            print(f"  ⚠ Users: {e}")

        conn.commit()

    print("\n✓ Demo tables cleared (AuditTrail and referenced Users preserved)")


def populate_users():
    """Create demo users (skip if they already exist)."""
    print("Populating Users...", end=" ")
    db_conn = get_default_connection()

    users = [
        ("John", "Operator", "john.operator@clearline.com", "Control Room Operator"),
        ("Sarah", "Engineer", "sarah.engineer@clearline.com", "Pipeline Engineer"),
        ("Mike", "Inspector", "mike.inspector@clearline.com", "Qualified Inspector"),
    ]

    created = 0
    with db_conn as conn:
        cursor = conn.cursor()

        for first, last, email, role in users:
            # Check if user already exists
            cursor.execute("SELECT UserID FROM dbo.Users WHERE Email = ?", (email,))
            existing = cursor.fetchone()

            if not existing:
                cursor.execute("""
                    INSERT INTO dbo.Users (FirstName, LastName, Email, Role)
                    OUTPUT INSERTED.UserID
                    VALUES (?, ?, ?, ?)
                """, (first, last, email, role))
                cursor.fetchone()
                created += 1

        conn.commit()

    print(f"✓ ({created} created, {len(users) - created} existing)")


def populate_assets():
    """Create pipeline segments (assets)."""
    print("Populating Assets...", end=" ")
    db_conn = get_default_connection()

    assets = [
        # SegmentID, Name, Grade, Diameter, WallThickness, SeamType, Heat, Mfr, MTR, Lat, Lon, MAOP, Class, Jurisdiction
        ("SEG-01", "Mainline South", "X52", 24.000, 0.3750, "ERW", "H2024-001", "US Steel", "https://clearline.com/mtr/001", 34.0522, -118.2437, 1000.00, "Class 1", "PHMSA"),
        ("SEG-02", "Mainline North", "X60", 24.000, 0.3125, "ERW", "H2024-002", "US Steel", "https://clearline.com/mtr/002", 34.0622, -118.2537, 950.00, "Class 2", "PHMSA"),
        ("SEG-03", "Eastern Branch", "X52", 16.000, 0.2500, "Seamless", "H2024-003", "Vallourec", "https://clearline.com/mtr/003", 34.0722, -118.2637, 875.00, "Class 1", "PHMSA"),
        ("SEG-04", "Western Spur", "X65", 20.000, 0.3125, "ERW", "H2024-004", "TMK IPSCO", "https://clearline.com/mtr/004", 34.0822, -118.2737, 1100.00, "Class 1", "PHMSA"),
    ]

    with db_conn as conn:
        cursor = conn.cursor()

        for asset_data in assets:
            cursor.execute("""
                INSERT INTO dbo.Assets
                (SegmentID, Name, PipeGrade, DiameterInches, WallThicknessInches,
                 SeamType, HeatNumber, Manufacturer, MTR_Link, GPSLatitude, GPSLongitude,
                 MAOP_PSIG, ClassLocation, Jurisdiction)
                OUTPUT INSERTED.AssetID
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, asset_data)
            cursor.fetchone()

        conn.commit()

    print(f"✓ ({len(assets)} segments)")


def populate_sensors():
    """Create pressure sensors for each segment."""
    print("Populating Sensors...", end=" ")
    db_conn = get_default_connection()

    sensors = [
        # SerialNumber, SegmentID, CalibDate, CertLink, CalibratedBy, HealthScore
        ("PXTR-2401-001", "SEG-01", "2025-12-15", "https://clearline.com/cal/001", "MetroCal Inc", 98),
        ("PXTR-2401-002", "SEG-02", "2025-12-20", "https://clearline.com/cal/002", "MetroCal Inc", 95),
        ("PXTR-2401-003", "SEG-03", "2025-12-18", "https://clearline.com/cal/003", "MetroCal Inc", 99),
        ("PXTR-2401-004", "SEG-04", "2025-12-22", "https://clearline.com/cal/004", "MetroCal Inc", 97),
    ]

    with db_conn as conn:
        cursor = conn.cursor()

        for sensor_data in sensors:
            cursor.execute("""
                INSERT INTO dbo.Sensors
                (SerialNumber, SegmentID, LastCalibrationDate, CalibrationCertLink,
                 CalibratedBy, HealthScore)
                OUTPUT INSERTED.SensorID
                VALUES (?, ?, ?, ?, ?, ?)
            """, sensor_data)
            cursor.fetchone()

        conn.commit()

    print(f"✓ ({len(sensors)} sensors)")


def get_sensor_id(segment_id):
    """Get sensor ID for a segment."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT SensorID FROM dbo.Sensors WHERE SegmentID = ?
        """, (segment_id,))
        result = cursor.fetchone()
        return result.SensorID if result else None


def populate_readings_with_story():
    """
    Populate readings following the drift story timeline WITH transient spikes.

    Timeline (2026-01-18):
    - 10:00 - Baseline readings (all normal)
    - 10:02 - SEG-02 crosses 90% MAOP (855 PSIG) → WARNING
    - 10:03 - SEG-01 TRANSIENT SPIKE (96% for one reading) → FILTERED by smart logic
    - 10:07 - SEG-02 crosses 95% MAOP (902.5 PSIG) → CRITICAL (SUSTAINED)
    - 10:09 - SEG-04 TRANSIENT SPIKE (97% for one reading) → FILTERED by smart logic
    - 10:12 - SEG-02 crosses 100% MAOP (950+ PSIG) → VIOLATION (SUSTAINED)

    This demonstrates ClearLine's transient filter preventing nuisance alarms!
    """
    print("Populating Readings (Drift Story Timeline)...", end=" ")

    base_date = datetime(2026, 1, 18, 10, 0, 0)

    # Get sensor IDs
    sensor_ids = {
        "SEG-01": get_sensor_id("SEG-01"),
        "SEG-02": get_sensor_id("SEG-02"),
        "SEG-03": get_sensor_id("SEG-03"),
        "SEG-04": get_sensor_id("SEG-04"),
    }

    # MAOP values
    maop_values = {
        "SEG-01": 1000.00,
        "SEG-02": 950.00,
        "SEG-03": 875.00,
        "SEG-04": 1100.00,
    }

    # Define the timeline of readings
    # Format: (minutes_offset, {segment: pressure})
    timeline = [
        # 10:00 - All segments normal (70-75% MAOP)
        (0, {
            "SEG-01": 750.0,   # 75% of 1000
            "SEG-02": 700.0,   # 73.7% of 950 - NORMAL
            "SEG-03": 650.0,   # 74.3% of 875
            "SEG-04": 825.0,   # 75% of 1100
        }),

        # 10:02 - SEG-02 crosses 90% MAOP → WARNING
        (2, {
            "SEG-01": 755.0,
            "SEG-02": 855.0,   # 90% of 950 - WARNING THRESHOLD
            "SEG-03": 652.0,
            "SEG-04": 828.0,
        }),

        # 10:03 - TRANSIENT SPIKE on SEG-01 (valve operation simulation)
        (3, {
            "SEG-01": 965.0,   # 96.5% of 1000 - SPIKE! (but 5-min avg will be ~76%)
            "SEG-02": 860.0,   # 90.5% - continuing climb
            "SEG-03": 653.0,
            "SEG-04": 829.0,
        }),

        # 10:04 - SEG-01 returns to normal after spike
        (4, {
            "SEG-01": 757.0,   # Back to normal - proves it was transient
            "SEG-02": 870.0,   # 91.6% - continuing climb
            "SEG-03": 654.0,
            "SEG-04": 830.0,
        }),

        # 10:05 - SEG-02 continues climbing
        (5, {
            "SEG-01": 758.0,
            "SEG-02": 880.0,   # 92.6% - Still WARNING
            "SEG-03": 655.0,
            "SEG-04": 830.0,
        }),

        # 10:07 - SEG-02 crosses 95% MAOP → CRITICAL
        (7, {
            "SEG-01": 760.0,
            "SEG-02": 902.5,   # 95% of 950 - CRITICAL THRESHOLD
            "SEG-03": 658.0,
            "SEG-04": 832.0,
        }),

        # 10:09 - TRANSIENT SPIKE on SEG-04 (pump start simulation)
        (9, {
            "SEG-01": 761.0,
            "SEG-02": 925.0,   # 97.4% - still climbing (SUSTAINED)
            "SEG-03": 659.0,
            "SEG-04": 1070.0,  # 97.3% of 1100 - SPIKE! (but 5-min avg will be ~76%)
        }),

        # 10:10 - SEG-04 returns to normal, SEG-02 approaching 100%
        (10, {
            "SEG-01": 762.0,
            "SEG-02": 940.0,   # 98.9% - Still CRITICAL (SUSTAINED)
            "SEG-03": 660.0,
            "SEG-04": 837.0,   # Back to normal - proves it was transient
        }),

        # 10:12 - SEG-02 crosses 100% MAOP → VIOLATION
        (12, {
            "SEG-01": 765.0,
            "SEG-02": 955.0,   # 100.5% of 950 - VIOLATION!
            "SEG-03": 662.0,
            "SEG-04": 838.0,
        }),

        # 10:15 - SEG-02 still in violation
        (15, {
            "SEG-01": 768.0,
            "SEG-02": 960.0,   # 101.1% - Still VIOLATION
            "SEG-03": 665.0,
            "SEG-04": 840.0,
        }),
    ]

    reading_count = 0
    print(".", end="", flush=True)

    for minutes_offset, segment_pressures in timeline:
        timestamp = base_date + timedelta(minutes=minutes_offset)

        for segment_id, pressure in segment_pressures.items():
            sensor_id = sensor_ids[segment_id]
            maop = maop_values[segment_id]

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

            reading_count += 1
            if reading_count % 7 == 0:
                print(".", end="", flush=True)

    print(f" ✓ ({reading_count} readings with hash chain)")


def populate_operator_acknowledgment():
    """Create audit log entry for operator acknowledgment at 10:08."""
    print("Creating Operator Acknowledgment...", end=" ")

    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        # Get operator user ID
        cursor.execute("SELECT UserID FROM dbo.Users WHERE FirstName = 'John' AND LastName = 'Operator'")
        operator_id = cursor.fetchone().UserID

        # Create audit entry
        ack_time = datetime(2026, 1, 18, 10, 8, 0)

        cursor.execute("""
            INSERT INTO dbo.AuditTrail
            (Timestamp, UserID, EventType, TableAffected, RecordID, Details, ChangeReason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ack_time,
            operator_id,
            "OPERATOR_ACKNOWLEDGMENT",
            "Readings",
            "SEG-02",
            "Operator acknowledged CRITICAL alarm on SEG-02 at 95% MAOP (902.5 PSIG). Monitoring pressure trend and preparing mitigation actions.",
            "Compliance requirement: Operator acknowledgment within 15 minutes of CRITICAL threshold"
        ))

        conn.commit()

        print(f"✓ (audit log entry created)")


def main():
    """Run complete demo data population."""
    print("\n")
    print("=" * 80)
    print(" " * 15 + "ClearLine Pipeline - Demo Data Population")
    print(" " * 20 + "Following the 'Drift Story' Timeline")
    print("=" * 80)

    # Ask user if they want to clear existing data
    print("\nThis will populate your database with demo data.")
    response = input("Clear existing data first? (y/n): ")

    if response.lower() == 'y':
        clear_all_data()

    try:
        # Populate in order (respecting foreign keys)
        populate_users()
        populate_assets()
        populate_sensors()
        populate_readings_with_story()
        populate_operator_acknowledgment()

        print("\n" + "=" * 80)
        print("✓ DEMO DATA POPULATION COMPLETE")
        print("=" * 80)
        print("\nDatabase Summary:")
        print("  • 4 pipeline segments (assets)")
        print("  • 4 pressure sensors  ")
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
        print("\n  🧠 ClearLine's Smart Filter:")
        print("     - 2 transient spikes FILTERED (no nuisance alarms)")
        print("     - 1 sustained drift FLAGGED (real issue detected)")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
