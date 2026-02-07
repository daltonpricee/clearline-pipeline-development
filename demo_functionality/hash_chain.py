"""
Immutable Hash Chain Implementation for Forensic Data Integrity.

This module provides functions to create and verify a blockchain-style hash chain
for pipeline pressure readings, ensuring tamper-proof data storage.
"""
import hashlib
from datetime import datetime
from db_config import get_default_connection


def generate_reading_hash(timestamp, segment_id, sensor_id, pressure_psig,
                          maop_psig, recorded_by, data_source, previous_hash=""):
    """
    Generate SHA-256 hash for a reading using blockchain-style chaining.

    Args:
        timestamp: Reading timestamp (datetime or string)
        segment_id: Segment identifier
        sensor_id: Sensor ID
        pressure_psig: Pressure reading in PSIG
        maop_psig: Maximum Allowable Operating Pressure
        recorded_by: Who recorded the data
        data_source: Source of the data (e.g., SCADA)
        previous_hash: Hash of the previous reading in the chain (empty for first reading)

    Returns:
        64-character SHA-256 hash string
    """
    # Convert timestamp to string if it's a datetime object
    if isinstance(timestamp, datetime):
        timestamp_str = timestamp.isoformat()
    else:
        timestamp_str = str(timestamp)

    # Create the data string by concatenating all fields
    data_string = (
        f"{timestamp_str}|"
        f"{segment_id}|"
        f"{sensor_id}|"
        f"{pressure_psig}|"
        f"{maop_psig}|"
        f"{recorded_by}|"
        f"{data_source}|"
        f"{previous_hash}"
    )

    # Generate SHA-256 hash
    hash_object = hashlib.sha256(data_string.encode('utf-8'))
    return hash_object.hexdigest()


def get_latest_hash():
    """
    Get the hash from the most recent reading in the database.

    Returns:
        Hash string of the latest reading, or empty string if no readings exist
    """
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT TOP 1 hash_signature
            FROM dbo.Readings
            ORDER BY ReadingID DESC
        """
        cursor.execute(query)
        result = cursor.fetchone()

        if result and result.hash_signature:
            return result.hash_signature
        return ""


def insert_reading_with_hash(timestamp, segment_id, sensor_id, pressure_psig,
                             maop_psig, recorded_by="SCADA", data_source="SCADA",
                             data_quality="GOOD", notes=None):
    """
    Insert a new reading into the database with hash chain verification.

    Args:
        timestamp: Reading timestamp (datetime object or ISO string)
        segment_id: Segment identifier
        sensor_id: Sensor ID
        pressure_psig: Pressure reading in PSIG
        maop_psig: Maximum Allowable Operating Pressure
        recorded_by: Who recorded the data (default: SCADA)
        data_source: Source of the data (default: SCADA)
        data_quality: Data quality flag (default: GOOD)
        notes: Optional notes

    Returns:
        Tuple of (ReadingID, hash_signature) for the inserted reading
    """
    # Get the previous hash
    previous_hash = get_latest_hash()

    # Generate hash for this reading
    current_hash = generate_reading_hash(
        timestamp, segment_id, sensor_id, pressure_psig,
        maop_psig, recorded_by, data_source, previous_hash
    )

    # Insert the reading
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        query = """
            INSERT INTO dbo.Readings
            (Timestamp, SegmentID, SensorID, PressurePSIG, MAOP_PSIG,
             RecordedBy, DataSource, DataQuality, Notes, hash_signature)
            OUTPUT INSERTED.ReadingID
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(query, (
            timestamp, segment_id, sensor_id, pressure_psig, maop_psig,
            recorded_by, data_source, data_quality, notes, current_hash
        ))

        reading_id = cursor.fetchone()[0]
        conn.commit()

    return reading_id, current_hash


def verify_hash_chain(verbose=False):
    """
    Verify the integrity of the entire hash chain.

    Args:
        verbose: If True, print detailed verification info for each reading

    Returns:
        Tuple of (is_valid: bool, broken_at_reading_id: int or None, total_checked: int)
    """
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        # Get all readings ordered by ReadingID
        query = """
            SELECT ReadingID, Timestamp, SegmentID, SensorID, PressurePSIG,
                   MAOP_PSIG, RecordedBy, DataSource, hash_signature
            FROM dbo.Readings
            ORDER BY ReadingID ASC
        """
        cursor.execute(query)
        readings = cursor.fetchall()

        if not readings:
            return True, None, 0

        previous_hash = ""
        total_checked = 0

        for reading in readings:
            # Calculate what the hash should be
            expected_hash = generate_reading_hash(
                reading.Timestamp,
                reading.SegmentID,
                reading.SensorID,
                reading.PressurePSIG,
                reading.MAOP_PSIG,
                reading.RecordedBy,
                reading.DataSource,
                previous_hash
            )

            # Compare with stored hash
            if reading.hash_signature != expected_hash:
                if verbose:
                    print(f"❌ HASH CHAIN BROKEN at ReadingID {reading.ReadingID}")
                    print(f"   Expected: {expected_hash}")
                    print(f"   Found:    {reading.hash_signature}")
                return False, reading.ReadingID, total_checked

            if verbose:
                print(f"✓ ReadingID {reading.ReadingID}: Hash valid")

            previous_hash = reading.hash_signature
            total_checked += 1

        return True, None, total_checked


def rebuild_hash_chain():
    """
    Rebuild the entire hash chain for existing readings.
    WARNING: This will update all hash_signature values in the database.
    Use this only for initial setup or after data migration.

    Returns:
        Number of readings updated
    """
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        # Get all readings ordered by ReadingID
        query = """
            SELECT ReadingID, Timestamp, SegmentID, SensorID, PressurePSIG,
                   MAOP_PSIG, RecordedBy, DataSource
            FROM dbo.Readings
            ORDER BY ReadingID ASC
        """
        cursor.execute(query)
        readings = cursor.fetchall()

        previous_hash = ""
        updated_count = 0

        for reading in readings:
            # Generate new hash
            new_hash = generate_reading_hash(
                reading.Timestamp,
                reading.SegmentID,
                reading.SensorID,
                reading.PressurePSIG,
                reading.MAOP_PSIG,
                reading.RecordedBy,
                reading.DataSource,
                previous_hash
            )

            # Update the hash in database
            update_query = """
                UPDATE dbo.Readings
                SET hash_signature = ?
                WHERE ReadingID = ?
            """
            cursor.execute(update_query, (new_hash, reading.ReadingID))

            previous_hash = new_hash
            updated_count += 1

        conn.commit()

    return updated_count
