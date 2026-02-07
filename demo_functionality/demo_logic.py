import json
from datetime import datetime
from db_config import get_default_connection

def load_rules(rules_file="rules.json"):
    """Load rules from JSON file."""
    with open(rules_file, 'r') as f:
        rules = json.load(f)
    return rules['rules']['maop_compliance']['thresholds']

def evaluate_status(pressure_psig, maop_psig, thresholds):
    """
    Determine status based on pressure relative to MAOP.
    
    Args:
        pressure_psig: Current pressure in psig
        maop_psig: Maximum Allowable Operating Pressure in psig
        thresholds: List of threshold rules from rules.json
        
    Returns:
        Status string: OK, WARNING, CRITICAL, or VIOLATION
    """
    if maop_psig <= 0:
        raise ValueError(f"Invalid MAOP: {maop_psig}")
    
    ratio = pressure_psig / maop_psig
    
    # Sort thresholds highest to lowest
    sorted_thresholds = sorted(thresholds, key=lambda x: x['threshold_ratio'], reverse=True)
    
    # Check each threshold from highest to lowest
    for threshold in sorted_thresholds:
        if ratio >= threshold['threshold_ratio']:
            return threshold['status']
    
    return "OK"

def load_assets():
    """Load assets from database Assets table."""
    assets = {}
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT SegmentID, Name, MAOP_PSIG, Jurisdiction
            FROM dbo.Assets
            ORDER BY SegmentID
        """
        cursor.execute(query)

        for row in cursor.fetchall():
            segment_id = row.SegmentID
            assets[segment_id] = {
                'segment_id': segment_id,
                'name': row.Name,
                'maop_psig': float(row.MAOP_PSIG),
                'jurisdiction': row.Jurisdiction
            }

    return assets

def load_telemetry():
    """Load telemetry from database Readings table."""
    telemetry = []
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT Timestamp, SegmentID, PressurePSIG
            FROM dbo.Readings
            WHERE DataQuality = 'GOOD'
            ORDER BY Timestamp, SegmentID
        """
        cursor.execute(query)

        for row in cursor.fetchall():
            # Convert datetime2 to ISO format string
            timestamp_str = row.Timestamp.isoformat() + 'Z'
            telemetry.append({
                'ts': timestamp_str,
                'segment_id': row.SegmentID,
                'pressure_psig': float(row.PressurePSIG)
            })

    return telemetry

def get_latest_pressure(segment_id, target_time, telemetry):
    """Get the most recent pressure reading for a segment at or before target_time."""
    target_dt = datetime.fromisoformat(target_time.replace('Z', '+00:00'))
    
    latest_reading = None
    latest_dt = None
    
    for reading in telemetry:
        if reading['segment_id'] != segment_id:
            continue
            
        reading_dt = datetime.fromisoformat(reading['ts'].replace('Z', '+00:00'))
        
        if reading_dt <= target_dt:
            if latest_dt is None or reading_dt > latest_dt:
                latest_reading = reading
                latest_dt = reading_dt
    
    return latest_reading

def evaluate_at_time(target_time, assets, telemetry, thresholds, evaluate_status_func):
    """Evaluate all segments at a specific time."""
    results = []
    
    for segment_id, asset in assets.items():
        reading = get_latest_pressure(segment_id, target_time, telemetry)
        
        if reading:
            pressure = reading['pressure_psig']
            maop = asset['maop_psig']
            status = evaluate_status_func(pressure, maop, thresholds)
            ratio = pressure / maop
            
            results.append({
                'segment_id': segment_id,
                'name': asset['name'],
                'pressure_psig': pressure,
                'maop_psig': maop,
                'ratio': ratio,
                'status': status,
                'reading_time': reading['ts']
            })
        else:
            results.append({
                'segment_id': segment_id,
                'name': asset['name'],
                'pressure_psig': None,
                'maop_psig': asset['maop_psig'],
                'ratio': None,
                'status': 'NO_DATA',
                'reading_time': None
            })
    
    return results

def print_results_table(results, target_time):
    """Print results in a formatted table."""
    print("\n" + "=" * 95)
    print(f"ClearLine Pipeline - MAOP Compliance Status at {target_time}")
    print("=" * 95)
    print(f"{'Segment':<12} {'Name':<20} {'Pressure':<15} {'MAOP':<15} {'% MAOP':<10} {'Status':<12}")
    print("-" * 95)
    
    for result in results:
        segment = result['segment_id']
        name = result['name'][:18]
        
        if result['pressure_psig'] is not None:
            pressure = f"{result['pressure_psig']:.1f} psig"
            maop = f"{result['maop_psig']:.1f} psig"
            ratio = f"{result['ratio']:.1%}"
        else:
            pressure = "N/A"
            maop = f"{result['maop_psig']:.1f} psig"
            ratio = "N/A"
        
        status = result['status']
        
        print(f"{segment:<12} {name:<20} {pressure:<15} {maop:<15} {ratio:<10} {status:<12}")
    
    print("=" * 95)