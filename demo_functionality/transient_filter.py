"""
Transient Filter (Logic Engine)
Distinguishes between transient pressure spikes and sustained drift.

This prevents "nuisance alarms" by only flagging readings when the
AVERAGE pressure over a time window stays elevated.
"""
import pandas as pd
from datetime import timedelta


def calculate_moving_average(df, window_minutes=5):
    """
    Calculate moving average pressure over a time window for each segment.

    Args:
        df: DataFrame with columns ['Timestamp', 'SegmentID', 'PressurePSIG', 'MAOP_PSIG']
        window_minutes: Time window for moving average (default: 5 minutes)

    Returns:
        DataFrame with additional columns:
        - MovingAvgPressure: Average pressure over the window
        - MovingAvgRatio: Average ratio over the window
        - IsTransient: True if this is a spike (single reading), False if sustained
        - AlertType: 'SPIKE' (ignore), 'SUSTAINED' (flag), or 'NORMAL'
    """
    df = df.copy()
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.sort_values(['SegmentID', 'Timestamp'])

    results = []

    for segment in df['SegmentID'].unique():
        segment_data = df[df['SegmentID'] == segment].copy()

        for idx, row in segment_data.iterrows():
            current_time = row['Timestamp']
            window_start = current_time - timedelta(minutes=window_minutes)

            # Get all readings in the time window (including current)
            window_data = segment_data[
                (segment_data['Timestamp'] >= window_start) &
                (segment_data['Timestamp'] <= current_time)
            ]

            # Calculate moving average
            avg_pressure = window_data['PressurePSIG'].mean()
            avg_ratio = (avg_pressure / row['MAOP_PSIG']) * 100

            # Determine if this is a transient spike or sustained drift
            current_ratio = (row['PressurePSIG'] / row['MAOP_PSIG']) * 100

            # Logic:
            # - If current reading is high (>95%) but moving avg is low (<95%) = SPIKE (ignore)
            # - If both current AND moving avg are high (>95%) = SUSTAINED (flag)
            # - Otherwise = NORMAL

            is_transient = False
            alert_type = 'NORMAL'

            if current_ratio >= 95:
                if avg_ratio >= 95:
                    alert_type = 'SUSTAINED'
                    is_transient = False
                else:
                    alert_type = 'SPIKE'
                    is_transient = True

            results.append({
                'ReadingID': row.get('ReadingID', None),
                'Timestamp': current_time,
                'SegmentID': row['SegmentID'],
                'SegmentName': row.get('SegmentName', ''),
                'PressurePSIG': row['PressurePSIG'],
                'MAOP_PSIG': row['MAOP_PSIG'],
                'InstantRatio': current_ratio,
                'MovingAvgPressure': avg_pressure,
                'MovingAvgRatio': avg_ratio,
                'WindowSize': len(window_data),
                'IsTransient': is_transient,
                'AlertType': alert_type
            })

    return pd.DataFrame(results)


def get_smart_alerts(df, window_minutes=5):
    """
    Get only SUSTAINED alerts (filters out transient spikes).

    Args:
        df: DataFrame with readings
        window_minutes: Time window for moving average

    Returns:
        DataFrame with only sustained drift alerts
    """
    filtered_data = calculate_moving_average(df, window_minutes)

    # Return only sustained alerts (ignore spikes)
    sustained_alerts = filtered_data[filtered_data['AlertType'] == 'SUSTAINED']

    return sustained_alerts


def get_spike_vs_sustained_summary(df, window_minutes=5):
    """
    Get summary statistics showing spike vs sustained detection.

    Returns:
        Dictionary with counts and examples
    """
    filtered_data = calculate_moving_average(df, window_minutes)

    total_high_readings = len(filtered_data[filtered_data['InstantRatio'] >= 95])
    spikes_filtered = len(filtered_data[filtered_data['AlertType'] == 'SPIKE'])
    sustained_flagged = len(filtered_data[filtered_data['AlertType'] == 'SUSTAINED'])

    return {
        'total_high_readings': total_high_readings,
        'spikes_filtered': spikes_filtered,
        'sustained_flagged': sustained_flagged,
        'filter_effectiveness': (spikes_filtered / total_high_readings * 100) if total_high_readings > 0 else 0,
        'filtered_data': filtered_data
    }


def classify_reading(current_pressure, maop, moving_avg_pressure, threshold_pct=95):
    """
    Classify a single reading as SPIKE, SUSTAINED, or NORMAL.

    Args:
        current_pressure: Current pressure reading
        maop: Maximum Allowable Operating Pressure
        moving_avg_pressure: Moving average pressure over time window
        threshold_pct: Threshold percentage (default: 95%)

    Returns:
        Tuple: (alert_type, is_transient, instant_ratio, avg_ratio)
    """
    instant_ratio = (current_pressure / maop) * 100
    avg_ratio = (moving_avg_pressure / maop) * 100

    if instant_ratio >= threshold_pct:
        if avg_ratio >= threshold_pct:
            return ('SUSTAINED', False, instant_ratio, avg_ratio)
        else:
            return ('SPIKE', True, instant_ratio, avg_ratio)
    else:
        return ('NORMAL', False, instant_ratio, avg_ratio)
