"""
Populate EngineeringReconciliation table with demo data.
Demonstrates the immutable ledger with version control and hash sealing.
"""
import hashlib
from datetime import datetime, timedelta
from db_config import get_default_connection


def calculate_reconciliation_hash(note_text, reading_id, timestamp, reconciler_id):
    """
    Calculate the ReconciliationHash - the seal on this note.
    Hash = SHA256(NoteText + ReadingID + Timestamp + ReconcilerID)
    """
    # Combine all fields into a single string
    data_string = f"{note_text}|{reading_id}|{timestamp}|{reconciler_id}"

    # Calculate SHA-256 hash
    hash_object = hashlib.sha256(data_string.encode('utf-8'))
    return hash_object.hexdigest()


def get_reading_hash(reading_id):
    """Get the hash_signature from a reading (the OriginalDataHash)."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT hash_signature FROM dbo.Readings WHERE ReadingID = ?
        """, (reading_id,))
        result = cursor.fetchone()
        return result[0] if result else None


def get_sample_readings():
    """Get some sample readings from the drift story for demo."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT TOP 10
                r.ReadingID,
                r.SegmentID,
                r.Timestamp,
                r.PressurePSIG,
                r.MAOP_PSIG,
                (r.PressurePSIG / r.MAOP_PSIG * 100) as Ratio,
                r.hash_signature
            FROM dbo.Readings r
            ORDER BY r.Timestamp
        """
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results


def add_engineering_note_with_hash(reconciler_id, reconciler_name, asset_id, qi_status,
                                   note_text, reading_id=None, supersedes_id=None):
    """
    Add engineering note with hash sealing.

    Args:
        reconciler_id: User ID of engineer
        reconciler_name: Full name of engineer
        asset_id: Segment ID (e.g., 'SEG-01')
        qi_status: QI status
        note_text: The engineering note
        reading_id: Optional reading being reconciled
        supersedes_id: Optional note ID this supersedes

    Returns:
        Tuple of (note_id, reconciliation_hash)
    """
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()

        # Get version number
        version_number = 1
        if supersedes_id:
            cursor.execute("""
                SELECT VersionNumber FROM dbo.EngineeringReconciliation
                WHERE NoteID = ?
            """, (supersedes_id,))
            result = cursor.fetchone()
            if result:
                version_number = result[0] + 1

        # Get original data hash if reading is specified
        original_data_hash = None
        if reading_id:
            original_data_hash = get_reading_hash(reading_id)

        # Get current timestamp
        timestamp = datetime.now()

        # Calculate reconciliation hash
        reconciliation_hash = calculate_reconciliation_hash(
            note_text,
            reading_id if reading_id else 0,
            timestamp.isoformat(),
            reconciler_id
        )

        # Insert note
        cursor.execute("""
            INSERT INTO dbo.EngineeringReconciliation
            (ReconcilerID, ReconcilerName, AssetID, QI_Status, NoteText,
             VersionNumber, Status, ReadingID, OriginalDataHash, ReconciliationHash)
            OUTPUT INSERTED.NoteID
            VALUES (?, ?, ?, ?, ?, ?, 'CURRENT', ?, ?, ?)
        """, (reconciler_id, reconciler_name, asset_id, qi_status, note_text,
              version_number, reading_id, original_data_hash, reconciliation_hash))

        note_id = cursor.fetchone()[0]

        # If superseding, mark old note
        if supersedes_id:
            cursor.execute("""
                UPDATE dbo.EngineeringReconciliation
                SET Status = 'SUPERSEDED', SupersededByID = ?
                WHERE NoteID = ?
            """, (note_id, supersedes_id))

        conn.commit()

        return (note_id, reconciliation_hash)


def populate_demo_reconciliation_notes():
    """Populate engineering reconciliation table with realistic demo data."""
    print("\n" + "=" * 80)
    print(" " * 15 + "Engineering Reconciliation - Demo Data Population")
    print("=" * 80 + "\n")

    db_conn = get_default_connection()

    # Get users
    with db_conn as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT UserID, FirstName + ' ' + LastName as Name FROM dbo.Users")
        users = {row.Name: row.UserID for row in cursor.fetchall()}

    if not users:
        print("❌ No users found. Please run populate_demo_data.py first.")
        return

    # Get sample readings
    readings = get_sample_readings()

    if not readings:
        print("❌ No readings found. Please run populate_demo_data.py first.")
        return

    print(f"✓ Found {len(readings)} readings to work with")
    print(f"✓ Found {len(users)} users")
    print("\nPopulating engineering notes...\n")

    notes_created = []

    # ========== DEMO SCENARIO 1: Transient Spike Explanation ==========
    # Find the SEG-01 transient spike reading (should be at 10:03 with 965.0 PSIG)
    spike_reading = next((r for r in readings if r['SegmentID'] == 'SEG-01' and r['Ratio'] > 95), None)

    if spike_reading:
        print("📝 Scenario 1: Explaining SEG-01 Transient Spike")
        print(f"   Reading ID: {spike_reading['ReadingID']} | Pressure: {spike_reading['PressurePSIG']:.1f} PSIG ({spike_reading['Ratio']:.1f}%)")

        # Initial note (wrong diagnosis)
        note_id_1, hash_1 = add_engineering_note_with_hash(
            reconciler_id=users[list(users.keys())[1]],  # Sarah Engineer
            reconciler_name=list(users.keys())[1],
            asset_id='SEG-01',
            qi_status='Pending',
            note_text=f"Sensor spike at {spike_reading['Timestamp']} to {spike_reading['PressurePSIG']:.1f} PSIG. Initial assessment: Lightning strike caused voltage surge affecting sensor A-101.",
            reading_id=spike_reading['ReadingID']
        )
        notes_created.append(note_id_1)
        print(f"   ✓ Version 1 created (NoteID: {note_id_1})")
        print(f"     ReconciliationHash: {hash_1[:16]}...")
        print(f"     OriginalDataHash: {spike_reading.get('hash_signature', 'N/A')[:16]}...")

        # Correction note (supersedes the first)
        print("\n   📝 Engineer reviews and finds mistake...")
        note_id_2, hash_2 = add_engineering_note_with_hash(
            reconciler_id=users[list(users.keys())[1]],
            reconciler_name=list(users.keys())[1],
            asset_id='SEG-01',
            qi_status='QI_Reviewing',
            note_text=f"CORRECTION to Note #{note_id_1}: Not lightning strike. Root cause: Power surge from compressor station start-up at 10:03 AM. Sensor A-101 experienced momentary voltage spike. Sensor returned to normal operation. Work Order #WO-2026-554 issued for electrical system review. No sensor replacement needed - spike was external, not sensor failure.",
            reading_id=spike_reading['ReadingID'],
            supersedes_id=note_id_1
        )
        notes_created.append(note_id_2)
        print(f"   ✓ Version 2 created (NoteID: {note_id_2}) - SUPERSEDES {note_id_1}")
        print(f"     ReconciliationHash: {hash_2[:16]}...")
        print(f"     Status: CURRENT (Version 1 now marked SUPERSEDED)")

    print("\n" + "-" * 80 + "\n")

    # ========== DEMO SCENARIO 2: SEG-02 Critical Alert Explanation ==========
    critical_reading = next((r for r in readings if r['SegmentID'] == 'SEG-02' and r['Ratio'] >= 95 and r['Ratio'] < 100), None)

    if critical_reading:
        print("📝 Scenario 2: SEG-02 Critical Pressure - Engineering Assessment")
        print(f"   Reading ID: {critical_reading['ReadingID']} | Pressure: {critical_reading['PressurePSIG']:.1f} PSIG ({critical_reading['Ratio']:.1f}%)")

        note_id_3, hash_3 = add_engineering_note_with_hash(
            reconciler_id=users[list(users.keys())[1]],
            reconciler_name=list(users.keys())[1],
            asset_id='SEG-02',
            qi_status='QI_Approved',
            note_text=f"SEG-02 reached {critical_reading['PressurePSIG']:.1f} PSIG ({critical_reading['Ratio']:.1f}% MAOP) at {critical_reading['Timestamp']}. Engineering assessment: Sustained pressure increase due to downstream valve partially closed. This is NOT a transient spike - 5-minute moving average confirms sustained elevation. Operator notified to investigate valve CV-202. Pressure trend monitored continuously. MAOP not exceeded but critical threshold breached. Valve adjustment recommended within 2 hours per SOP-MAOP-001.",
            reading_id=critical_reading['ReadingID']
        )
        notes_created.append(note_id_3)
        print(f"   ✓ Note created (NoteID: {note_id_3})")
        print(f"     ReconciliationHash: {hash_3[:16]}...")
        print(f"     QI Status: QI_Approved")
        print(f"     OriginalDataHash: {critical_reading.get('hash_signature', 'N/A')[:16]}...")

    print("\n" + "-" * 80 + "\n")

    # ========== DEMO SCENARIO 3: Normal Reading - Routine Check ==========
    normal_reading = next((r for r in readings if r['SegmentID'] == 'SEG-03'), None)

    if normal_reading:
        print("📝 Scenario 3: SEG-03 Routine Engineering Check")
        print(f"   Reading ID: {normal_reading['ReadingID']} | Pressure: {normal_reading['PressurePSIG']:.1f} PSIG ({normal_reading['Ratio']:.1f}%)")

        note_id_4, hash_4 = add_engineering_note_with_hash(
            reconciler_id=users[list(users.keys())[2]],  # Mike Inspector
            reconciler_name=list(users.keys())[2],
            asset_id='SEG-03',
            qi_status='Closed',
            note_text=f"Routine quarterly inspection completed for SEG-03. Pressure readings nominal at {normal_reading['PressurePSIG']:.1f} PSIG ({normal_reading['Ratio']:.1f}% MAOP). Sensor PXTR-2401-003 calibration verified against reference gauge (±0.2 PSIG tolerance). All safety margins maintained. No corrective actions required. Next inspection due: {(datetime.now() + timedelta(days=90)).strftime('%Y-%m-%d')}.",
            reading_id=normal_reading['ReadingID']
        )
        notes_created.append(note_id_4)
        print(f"   ✓ Note created (NoteID: {note_id_4})")
        print(f"     ReconciliationHash: {hash_4[:16]}...")
        print(f"     QI Status: Closed")

    print("\n" + "=" * 80)
    print("✓ ENGINEERING RECONCILIATION DEMO DATA COMPLETE")
    print("=" * 80)
    print(f"\nSummary:")
    print(f"  • {len(notes_created)} engineering notes created")
    print(f"  • Demonstrates version control (Note 1 → Note 2 supersession)")
    print(f"  • Each note sealed with ReconciliationHash (SHA-256)")
    print(f"  • OriginalDataHash preserves reading's hash_signature")
    print(f"  • Ready to view in dashboard: Engineering Reconciliation tab")
    print("\nDemo Highlights:")
    print("  1. Version Control: Note #1 superseded by Note #2 (correction)")
    print("  2. Hash Sealing: Every note cryptographically sealed")
    print("  3. Forensic Trail: OriginalDataHash anchors to raw sensor data")
    print("  4. QI Workflow: Notes progress through QI statuses")
    print("  5. Immutable: Try to edit a note in SSMS - triggers will block it!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        populate_demo_reconciliation_notes()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("  1. Database has been populated: python populate_demo_data.py")
        print("  2. EngineeringReconciliation table exists: run create_sticky_notes_table.sql")
        import traceback
        traceback.print_exc()
