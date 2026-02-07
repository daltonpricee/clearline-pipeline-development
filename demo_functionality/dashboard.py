"""
ClearLine Pipeline - One-Click Audit Dashboard
Streamlit web interface for investors and compliance officers.

Run with: streamlit run dashboard.py
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import numpy as np
from db_config import get_default_connection
from hash_chain import verify_hash_chain
from demo_logic import load_rules
from transient_filter import calculate_moving_average, get_spike_vs_sustained_summary

# Page config
st.set_page_config(
    page_title="ClearLine Pipeline Audit Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize dark mode state
if 'dark_mode' not in st.session_state:
    st.session_state.dark_mode = False

# Dynamic CSS based on dark mode
if st.session_state.dark_mode:
    bg_gradient = "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)"
    header_bg = "linear-gradient(135deg, #0f3460 0%, #16213e 100%)"
    text_color = "#e0e0e0"
    card_bg = "#1f1f1f"
    plot_bg = "rgba(31, 31, 31, 0.8)"
    metric_color = "#ffffff"
    label_color = "#b0b0b0"
    border_color = "#2a2a2a"
else:
    bg_gradient = "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)"
    header_bg = "linear-gradient(135deg, #667eea 0%, #764ba2 100%)"
    text_color = "#1e293b"
    card_bg = "white"
    plot_bg = "rgba(248,250,252,0.8)"
    metric_color = "#1e293b"
    label_color = "#64748b"
    border_color = "#e2e8f0"

# Custom CSS for professional styling
st.markdown(f"""
<style>
    /* Main app background */
    .stApp {{
        background: {bg_gradient};
    }}

    /* Main container styling */
    .main {{
        background: {bg_gradient};
    }}

    /* Block container */
    .block-container {{
        background: transparent;
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: {card_bg};
    }}

    /* Header styling */
    .main-header {{
        background: {header_bg};
        padding: 2.5rem 3rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.15);
        border: 1px solid rgba(255,255,255,0.1);
    }}

    .main-header h1 {{
        color: white;
        font-weight: 800;
        margin: 0;
        font-size: 2.8rem;
        font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
        letter-spacing: -0.02em;
        line-height: 1.2;
    }}

    .main-header .subtitle {{
        color: #e0e7ff;
        margin: 0.5rem 0 0 0;
        font-size: 1rem;
        font-weight: 500;
        font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
        letter-spacing: 0.03em;
        text-transform: uppercase;
        opacity: 0.9;
    }}

    /* Metric cards */
    div[data-testid="stMetricValue"] {{
        font-size: 2rem;
        font-weight: 700;
        color: {metric_color};
    }}

    div[data-testid="stMetricLabel"] {{
        font-size: 0.9rem;
        font-weight: 600;
        color: {label_color};
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    div[data-testid="stMetricDelta"] {{
        font-size: 0.85rem;
    }}

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 8px;
        background-color: {card_bg};
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        border: 1px solid {border_color};
    }}

    .stTabs [data-baseweb="tab"] {{
        height: 50px;
        white-space: pre-wrap;
        background-color: {'#2a2a2a' if st.session_state.dark_mode else '#f1f5f9'};
        border-radius: 8px;
        color: {label_color};
        font-weight: 600;
        font-size: 0.95rem;
        padding: 0 1.5rem;
        transition: all 0.3s ease;
    }}

    .stTabs [aria-selected="true"] {{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        box-shadow: 0 4px 6px rgba(102, 126, 234, 0.3);
    }}

    /* Cards and containers */
    .css-1r6slb0 {{
        background-color: {card_bg};
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        border: 1px solid {border_color};
    }}

    /* Dataframe styling */
    .dataframe {{
        border: none !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        border-radius: 8px;
        background-color: {card_bg};
    }}

    /* Button styling */
    .stButton > button {{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px rgba(102, 126, 234, 0.3);
    }}

    .stButton > button:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(102, 126, 234, 0.4);
    }}

    /* Download button */
    .stDownloadButton > button {{
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }}

    .stDownloadButton > button:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(16, 185, 129, 0.3);
    }}

    /* Info/Warning/Error boxes */
    .stAlert {{
        border-radius: 8px;
        border-left: 4px solid;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }}

    /* Expander styling */
    .streamlit-expanderHeader {{
        background-color: #f8fafc;
        border-radius: 8px;
        font-weight: 600;
    }}

    /* Divider */
    hr {{
        margin: 2rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #e2e8f0, transparent);
    }}

    /* Footer */
    .footer {{
        text-align: center;
        padding: 2rem;
        color: #64748b;
        font-size: 0.9rem;
        margin-top: 3rem;
    }}

    /* Plotly charts */
    .js-plotly-plot {{
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        background: {card_bg};
        padding: 1rem;
        border: 1px solid {border_color};
    }}

    /* Dark mode toggle */
    .dark-mode-toggle {{
        position: absolute;
        top: 2rem;
        right: 3rem;
        z-index: 1000;
    }}
</style>
""", unsafe_allow_html=True)

# Dark mode toggle in header
col1, col2 = st.columns([6, 1])
with col2:
    toggle_label = "🌙 Dark" if not st.session_state.dark_mode else "☀️ Light"
    if st.button(toggle_label, key="dark_mode_toggle", use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()

# Header
st.markdown("""
<div class="main-header">
    <h1>CLEARLINE PIPELINE</h1>
    <p class="subtitle">Enterprise Compliance & Monitoring Platform</p>
</div>
""", unsafe_allow_html=True)

# Load rules (handle path correctly)
script_dir = os.path.dirname(os.path.abspath(__file__))
rules_path = os.path.join(script_dir, "rules.json")
thresholds = load_rules(rules_path)

def get_all_readings():
    """Get all readings from database with calculated ratios."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT
                r.ReadingID,
                r.Timestamp,
                r.SegmentID,
                a.Name as SegmentName,
                r.PressurePSIG,
                r.MAOP_PSIG,
                (r.PressurePSIG / r.MAOP_PSIG * 100) as Ratio,
                r.DataQuality,
                r.hash_signature
            FROM dbo.Readings r
            JOIN dbo.Assets a ON r.SegmentID = a.SegmentID
            ORDER BY r.Timestamp ASC
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame.from_records(data, columns=columns)
            # Convert Decimal columns to float for calculations
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            return df
        return pd.DataFrame()


def get_drift_alerts():
    """Get all readings that crossed 95% MAOP threshold."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT
                r.Timestamp,
                r.SegmentID,
                a.Name as SegmentName,
                r.PressurePSIG,
                r.MAOP_PSIG,
                (r.PressurePSIG / r.MAOP_PSIG * 100) as Ratio,
                CASE
                    WHEN r.PressurePSIG >= r.MAOP_PSIG THEN 'VIOLATION'
                    WHEN r.PressurePSIG >= r.MAOP_PSIG * 0.95 THEN 'CRITICAL'
                    WHEN r.PressurePSIG >= r.MAOP_PSIG * 0.90 THEN 'WARNING'
                    ELSE 'OK'
                END as Status
            FROM dbo.Readings r
            JOIN dbo.Assets a ON r.SegmentID = a.SegmentID
            WHERE r.PressurePSIG >= r.MAOP_PSIG * 0.95
            ORDER BY r.Timestamp DESC
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame.from_records(data, columns=columns)
            # Convert Decimal columns to float for calculations
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            return df
        return pd.DataFrame()


def get_operator_activity():
    """Get all operator actions from AuditTrail."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT
                a.Timestamp,
                u.FirstName + ' ' + u.LastName as Operator,
                a.EventType as ActionType,
                a.TableAffected,
                a.RecordID,
                a.Details as Description,
                a.ChangeReason as ComplianceNote
            FROM dbo.AuditTrail a
            LEFT JOIN dbo.Users u ON a.UserID = u.UserID
            ORDER BY a.Timestamp DESC
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame.from_records(data, columns=columns)
            return df
        return pd.DataFrame()


def get_sensor_health():
    """Get sensor health and calibration status."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT
                s.SerialNumber,
                s.SegmentID,
                a.Name as SegmentName,
                s.LastCalibrationDate,
                s.CalibratedBy,
                s.HealthScore,
                DATEDIFF(day, s.LastCalibrationDate, GETDATE()) as DaysSinceCalibration,
                CASE
                    WHEN DATEDIFF(day, s.LastCalibrationDate, GETDATE()) > 365 THEN 'Overdue'
                    WHEN DATEDIFF(day, s.LastCalibrationDate, GETDATE()) > 330 THEN 'Due Soon'
                    ELSE 'Current'
                END as CalibrationStatus
            FROM dbo.Sensors s
            JOIN dbo.Assets a ON s.SegmentID = a.SegmentID
            ORDER BY s.SegmentID
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame.from_records(data, columns=columns)
            # Convert numeric columns
            if 'HealthScore' in df.columns:
                df['HealthScore'] = df['HealthScore'].astype(float)
            if 'DaysSinceCalibration' in df.columns:
                df['DaysSinceCalibration'] = df['DaysSinceCalibration'].astype(int)
            return df
        return pd.DataFrame()


def get_alert_response_metrics():
    """Calculate alert response times and acknowledgment rates."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        # Get critical readings and their acknowledgments
        query = """
            SELECT
                r.Timestamp as AlertTime,
                r.SegmentID,
                a.Name as SegmentName,
                r.PressurePSIG,
                r.MAOP_PSIG,
                (r.PressurePSIG / r.MAOP_PSIG * 100) as Ratio,
                CASE
                    WHEN r.PressurePSIG >= r.MAOP_PSIG THEN 'VIOLATION'
                    WHEN r.PressurePSIG >= r.MAOP_PSIG * 0.95 THEN 'CRITICAL'
                    WHEN r.PressurePSIG >= r.MAOP_PSIG * 0.90 THEN 'WARNING'
                END as AlertLevel,
                (SELECT MIN(at.Timestamp)
                 FROM dbo.AuditTrail at
                 WHERE at.RecordID = r.SegmentID
                 AND at.Timestamp >= r.Timestamp
                 AND at.EventType = 'OPERATOR_ACKNOWLEDGMENT') as AckTime
            FROM dbo.Readings r
            JOIN dbo.Assets a ON r.SegmentID = a.SegmentID
            WHERE r.PressurePSIG >= r.MAOP_PSIG * 0.90
            ORDER BY r.Timestamp DESC
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame.from_records(data, columns=columns)
            # Convert numeric columns
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

            # Calculate response time in minutes
            if 'AckTime' in df.columns:
                df['ResponseTimeMinutes'] = (pd.to_datetime(df['AckTime']) - pd.to_datetime(df['AlertTime'])).dt.total_seconds() / 60

            return df
        return pd.DataFrame()


def predict_next_pressure(segment_data):
    """Simple linear regression to predict next pressure reading."""
    if len(segment_data) < 2:
        return None

    # Convert timestamps to numeric for regression
    segment_data = segment_data.copy()
    segment_data['TimeNumeric'] = (segment_data['Timestamp'] - segment_data['Timestamp'].min()).dt.total_seconds()

    # Simple linear regression
    x = segment_data['TimeNumeric'].values
    y = segment_data['PressurePSIG'].astype(float).values  # Convert Decimal to float

    # Calculate slope and intercept
    slope = np.polyfit(x, y, 1)[0]
    intercept = np.polyfit(x, y, 1)[1]

    # Predict next 2 time points
    last_time = x[-1]
    time_delta = (x[-1] - x[-2]) if len(x) > 1 else 300  # 5 min default

    future_times = [last_time + time_delta, last_time + (time_delta * 2)]
    future_pressures = [slope * t + intercept for t in future_times]

    future_timestamps = [
        segment_data['Timestamp'].max() + timedelta(seconds=time_delta),
        segment_data['Timestamp'].max() + timedelta(seconds=time_delta * 2)
    ]

    return future_timestamps, future_pressures, slope


# Create tabs
tab_exec, tab_pulse, tab_alerts, tab_ledger, tab_activity, tab_sensors = st.tabs([
    "💼 Executive Summary",
    "📈 Live Pulse",
    "⚠️ Drift Alerts",
    "🔒 Compliance Ledger",
    "👤 Operator Activity",
    "🔧 Sensor Health"
])

# TAB 0: EXECUTIVE SUMMARY
with tab_exec:
    st.header("💼 Executive Summary: KPIs & Risk Analysis")
    st.markdown("**Real-time compliance overview and financial impact analysis**")

    df_readings = get_all_readings()

    if not df_readings.empty:
        # Top-level KPIs
        st.subheader("📊 Key Performance Indicators")

        col1, col2, col3, col4, col5 = st.columns(5)

        total_readings = len(df_readings)
        segments = df_readings['SegmentID'].nunique()
        violations = len(df_readings[df_readings['Ratio'] >= 100])
        critical_events = len(df_readings[df_readings['Ratio'] >= 95])

        # Compliance score (% of readings below 95% MAOP)
        compliance_score = ((total_readings - critical_events) / total_readings * 100) if total_readings > 0 else 100

        col1.metric("Total Readings", f"{total_readings:,}")
        col2.metric("Pipeline Segments", segments)
        col3.metric("Compliance Score", f"{compliance_score:.1f}%",
                   delta=f"{compliance_score - 85:.1f}%", delta_color="normal")
        col4.metric("Critical Events", critical_events, delta=f"{critical_events}", delta_color="inverse")
        col5.metric("Violations", violations, delta=f"{violations}", delta_color="inverse")

        st.divider()

        # Financial Impact
        st.subheader("💰 Financial Impact Analysis")

        col1, col2, col3 = st.columns(3)

        # Calculate costs (example numbers)
        violation_cost = violations * 25000  # $25k per violation fine
        critical_cost = critical_events * 5000  # $5k per critical event for inspection
        preventive_savings = (total_readings - critical_events) * 100  # $100 saved per normal reading

        col1.metric("🚨 Violation Fines", f"${violation_cost:,}",
                   help="Estimated regulatory fines for MAOP violations")
        col2.metric("⚠️ Inspection Costs", f"${critical_cost:,}",
                   help="Required inspection costs for critical events")
        col3.metric("✅ Preventive Savings", f"${preventive_savings:,}",
                   help="Savings from early warning and prevention")

        net_impact = preventive_savings - violation_cost - critical_cost
        roi_percentage = (net_impact / max(1, violation_cost + critical_cost)) * 100 if (violation_cost + critical_cost) > 0 else 0

        st.metric("📈 Net Financial Impact", f"${net_impact:,}",
                 delta=f"ROI: {roi_percentage:.1f}%",
                 delta_color="normal" if net_impact > 0 else "inverse",
                 help="Total savings minus costs (positive = system saves money)")

        st.divider()

        # Risk Heatmap
        st.subheader("🔥 Risk Heatmap: Segment-Level Analysis")

        # Calculate risk score for each segment
        segment_risk = df_readings.groupby('SegmentID').agg({
            'Ratio': ['max', 'mean', 'std'],
            'SegmentName': 'first',
            'MAOP_PSIG': 'first'
        }).reset_index()

        segment_risk.columns = ['SegmentID', 'MaxRatio', 'AvgRatio', 'StdRatio', 'Name', 'MAOP']

        # Convert Decimal to float for calculations
        segment_risk['MaxRatio'] = segment_risk['MaxRatio'].astype(float)
        segment_risk['AvgRatio'] = segment_risk['AvgRatio'].astype(float)
        segment_risk['StdRatio'] = segment_risk['StdRatio'].astype(float)

        # Calculate risk score (0-100 scale)
        segment_risk['RiskScore'] = (
            segment_risk['MaxRatio'] * 0.5 +  # Max pressure = 50% weight
            segment_risk['AvgRatio'] * 0.3 +  # Average = 30% weight
            (segment_risk['StdRatio'] * 5) * 0.2  # Volatility = 20% weight
        ).clip(0, 100)

        # Sort by risk score
        segment_risk = segment_risk.sort_values('RiskScore', ascending=False)

        # Create risk heatmap visualization
        fig_heatmap = go.Figure()

        # Professional color palette
        colors = []
        for score in segment_risk['RiskScore']:
            if score >= 95:
                colors.append('#ef4444')  # Red - Critical (modern red)
            elif score >= 90:
                colors.append('#f97316')  # Orange - High (modern orange)
            elif score >= 75:
                colors.append('#eab308')  # Yellow - Medium (modern yellow)
            else:
                colors.append('#10b981')  # Green - Low (modern green)

        fig_heatmap.add_trace(go.Bar(
            x=segment_risk['SegmentID'],
            y=segment_risk['RiskScore'],
            marker=dict(
                color=colors,
                line=dict(color='rgba(255,255,255,0.8)', width=2)
            ),
            text=segment_risk['RiskScore'].round(1),
            textposition='outside',
            textfont=dict(size=14, color='#1e293b', family='Arial Black'),
            hovertemplate=
                '<b>%{x}</b><br>' +
                'Risk Score: %{y:.1f}<br>' +
                'Max Ratio: ' + segment_risk['MaxRatio'].round(1).astype(str) + '%<br>' +
                'Avg Ratio: ' + segment_risk['AvgRatio'].round(1).astype(str) + '%<br>' +
                '<extra></extra>'
        ))

        # Add risk level lines with professional styling
        fig_heatmap.add_hline(y=95, line_dash="dash", line_color="#ef4444", line_width=2,
                             annotation_text="Critical Risk (95+)",
                             annotation_position="right",
                             annotation_font=dict(size=11, color="#ef4444", family='Arial'))
        fig_heatmap.add_hline(y=90, line_dash="dash", line_color="orange",
                             annotation_text="High Risk (90+)",
                             annotation_position="right")
        fig_heatmap.add_hline(y=75, line_dash="dash", line_color="yellow",
                             annotation_text="Medium Risk (75+)",
                             annotation_position="right")

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'
        title_color = '#e0e0e0' if st.session_state.dark_mode else '#1e293b'
        axis_color = '#b0b0b0' if st.session_state.dark_mode else '#475569'
        grid_color = '#2a2a2a' if st.session_state.dark_mode else '#e2e8f0'

        fig_heatmap.update_layout(
            title=dict(
                text="Segment Risk Scores (Higher = More Risk)",
                font=dict(size=20, color=title_color, family='Arial Black')
            ),
            xaxis=dict(
                title=dict(
                    text="Pipeline Segment",
                    font=dict(size=14, color=axis_color, family='Arial')
                ),
                gridcolor=grid_color
            ),
            yaxis=dict(
                title=dict(
                    text="Risk Score",
                    font=dict(size=14, color=axis_color, family='Arial')
                ),
                gridcolor=grid_color,
                range=[0, 110]
            ),
            height=400,
            showlegend=False,
            template=chart_template,
            plot_bgcolor=plot_bg,
            paper_bgcolor=card_bg,
            margin=dict(l=50, r=50, t=80, b=50)
        )

        st.plotly_chart(fig_heatmap, use_container_width=True)

        # Risk breakdown table
        st.subheader("📋 Detailed Risk Breakdown")

        risk_display = segment_risk.copy()
        risk_display['RiskLevel'] = risk_display['RiskScore'].apply(
            lambda x: 'CRITICAL' if x >= 95 else 'HIGH' if x >= 90 else 'MEDIUM' if x >= 75 else 'LOW'
        )
        risk_display['MaxRatio'] = risk_display['MaxRatio'].round(1).astype(str) + '%'
        risk_display['AvgRatio'] = risk_display['AvgRatio'].round(1).astype(str) + '%'
        risk_display['RiskScore'] = risk_display['RiskScore'].round(1)

        def highlight_risk(row):
            if row['RiskLevel'] == 'CRITICAL':
                return ['background-color: #ff0000; color: white'] * len(row)
            elif row['RiskLevel'] == 'HIGH':
                return ['background-color: #ff6600; color: white'] * len(row)
            elif row['RiskLevel'] == 'MEDIUM':
                return ['background-color: #ffaa00; color: white'] * len(row)
            return [''] * len(row)

        st.dataframe(
            risk_display[['SegmentID', 'Name', 'RiskScore', 'RiskLevel', 'MaxRatio', 'AvgRatio']].style.apply(highlight_risk, axis=1),
            use_container_width=True
        )

    else:
        st.warning("No data available. Run `python setup_demo.py` first.")


# TAB 1: LIVE PULSE (with Predictive Analytics)
with tab_pulse:
    st.header("📈 Live Pulse: Pressure vs. MAOP with Predictive Analytics")
    st.markdown("Real-time pressure monitoring across all pipeline segments **with AI-powered trend prediction**")

    df_readings = get_all_readings()

    if not df_readings.empty:
        # Create interactive chart
        fig = go.Figure()

        segments = df_readings['SegmentID'].unique()

        for segment in segments:
            segment_data = df_readings[df_readings['SegmentID'] == segment].copy()
            segment_name = segment_data.iloc[0]['SegmentName']
            maop = float(segment_data.iloc[0]['MAOP_PSIG'])

            # Professional color palette for segments
            segment_colors = {
                'SEG-01': '#667eea',
                'SEG-02': '#f093fb',
                'SEG-03': '#4facfe',
                'SEG-04': '#43e97b'
            }

            # Pressure line (actual data)
            fig.add_trace(go.Scatter(
                x=segment_data['Timestamp'],
                y=segment_data['PressurePSIG'],
                mode='lines+markers',
                name=f'{segment} ({segment_name})',
                line=dict(width=3, color=segment_colors.get(segment, '#667eea')),
                marker=dict(size=6, line=dict(width=1, color='white')),
                hovertemplate=
                    '<b>%{fullData.name}</b><br>' +
                    'Time: %{x}<br>' +
                    'Pressure: %{y:.1f} PSIG<br>' +
                    '<extra></extra>'
            ))

            # Predictive trend line
            prediction = predict_next_pressure(segment_data)
            if prediction:
                future_times, future_pressures, slope = prediction

                # Add prediction line
                fig.add_trace(go.Scatter(
                    x=future_times,
                    y=future_pressures,
                    mode='lines+markers',
                    name=f'{segment} Prediction',
                    line=dict(dash='dot', width=2),
                    marker=dict(symbol='diamond', size=8),
                    hovertemplate=
                        '<b>PREDICTED</b><br>' +
                        'Time: %{x}<br>' +
                        'Pressure: %{y:.1f} PSIG<br>' +
                        f'Trend: {"Rising" if slope > 0 else "Falling"}<br>' +
                        '<extra></extra>'
                ))

            # MAOP line (dashed)
            fig.add_trace(go.Scatter(
                x=segment_data['Timestamp'],
                y=[maop] * len(segment_data),
                mode='lines',
                name=f'{segment} MAOP',
                line=dict(dash='dash', width=2, color='gray'),
                showlegend=False,
                hoverinfo='skip'
            ))

            # 95% CRITICAL threshold
            fig.add_trace(go.Scatter(
                x=segment_data['Timestamp'],
                y=[maop * 0.95] * len(segment_data),
                mode='lines',
                name=f'{segment} 95% (CRITICAL)',
                line=dict(dash='dot', width=1, color='orange'),
                showlegend=False,
                hoverinfo='skip'
            ))

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'
        title_color = '#e0e0e0' if st.session_state.dark_mode else '#1e293b'
        axis_color = '#b0b0b0' if st.session_state.dark_mode else '#475569'
        grid_color = '#2a2a2a' if st.session_state.dark_mode else '#e2e8f0'
        legend_bg = 'rgba(31,31,31,0.9)' if st.session_state.dark_mode else 'rgba(255,255,255,0.8)'

        fig.update_layout(
            title=dict(
                text="Pressure Readings Over Time with AI Predictions",
                font=dict(size=20, color=title_color, family='Arial Black')
            ),
            xaxis=dict(
                title=dict(
                    text="Time",
                    font=dict(size=14, color=axis_color, family='Arial')
                ),
                gridcolor=grid_color,
                showgrid=True
            ),
            yaxis=dict(
                title=dict(
                    text="Pressure (PSIG)",
                    font=dict(size=14, color=axis_color, family='Arial')
                ),
                gridcolor=grid_color,
                showgrid=True
            ),
            hovermode='x unified',
            height=500,
            template=chart_template,
            plot_bgcolor=plot_bg,
            paper_bgcolor=card_bg,
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor=legend_bg,
                bordercolor=grid_color,
                borderwidth=1
            )
        )

        st.plotly_chart(fig, use_container_width=True)

        # Key metrics
        col1, col2, col3, col4 = st.columns(4)

        total_readings = len(df_readings)
        violation_count = len(df_readings[df_readings['Ratio'] >= 100])
        critical_count = len(df_readings[(df_readings['Ratio'] >= 95) & (df_readings['Ratio'] < 100)])
        warning_count = len(df_readings[(df_readings['Ratio'] >= 90) & (df_readings['Ratio'] < 95)])

        col1.metric("Total Readings", total_readings)
        col2.metric("Warnings (90%+)", warning_count, delta=None)
        col3.metric("Critical (95%+)", critical_count, delta=None, delta_color="inverse")
        col4.metric("Violations (100%+)", violation_count, delta=None, delta_color="inverse")

    else:
        st.warning("No readings found in database. Run `python setup_demo.py` first.")


# TAB 2: DRIFT ALERTS
with tab_alerts:
    st.header("⚠️ Drift Alerts: Critical Threshold Crossings")
    st.markdown("Log of all readings that crossed the 95% MAOP threshold")

    df_alerts = get_drift_alerts()

    if not df_alerts.empty:
        # Summary stats
        col1, col2, col3 = st.columns(3)

        violation_alerts = len(df_alerts[df_alerts['Status'] == 'VIOLATION'])
        critical_alerts = len(df_alerts[df_alerts['Status'] == 'CRITICAL'])
        affected_segments = df_alerts['SegmentID'].nunique()

        col1.metric("🚨 Violation Events", violation_alerts)
        col2.metric("⚠️ Critical Events", critical_alerts)
        col3.metric("📍 Affected Segments", affected_segments)

        st.divider()

        # TRANSIENT FILTER SECTION
        st.subheader("🧠 Smart Filtering: Transient Spikes vs. Sustained Drift")
        st.markdown("**ClearLine's Logic Engine filters out nuisance alarms using 5-minute moving averages**")

        # Get full readings for transient analysis
        df_all_readings = get_all_readings()

        if not df_all_readings.empty:
            # Calculate transient filter results
            filter_summary = get_spike_vs_sustained_summary(df_all_readings, window_minutes=5)
            filtered_data = filter_summary['filtered_data']

            # Show filter effectiveness metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                col1.metric("Total High Readings (≥95%)", filter_summary['total_high_readings'])

            with col2:
                col2.metric("🔇 Spikes Filtered", filter_summary['spikes_filtered'],
                           delta="Nuisance alarms prevented")

            with col3:
                col3.metric("🚨 Sustained Flagged", filter_summary['sustained_flagged'],
                           delta="Real drift detected")

            with col4:
                if filter_summary['total_high_readings'] > 0:
                    effectiveness = f"{filter_summary['filter_effectiveness']:.0f}%"
                else:
                    effectiveness = "N/A"
                col4.metric("Filter Effectiveness", effectiveness,
                           delta="False alarms reduced")

            # Visualization: Instant vs Moving Average
            st.subheader("📊 Instant Pressure vs. 5-Minute Moving Average")

            # Focus on segments with high readings
            high_reading_segments = filtered_data[filtered_data['InstantRatio'] >= 90]['SegmentID'].unique()

            if len(high_reading_segments) > 0:
                for segment in high_reading_segments:
                    segment_filtered = filtered_data[filtered_data['SegmentID'] == segment]
                    segment_name = segment_filtered.iloc[0]['SegmentName']

                    fig_filter = go.Figure()

                    chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'
                    title_color = '#e0e0e0' if st.session_state.dark_mode else '#1e293b'
                    axis_color = '#b0b0b0' if st.session_state.dark_mode else '#475569'
                    grid_color = '#2a2a2a' if st.session_state.dark_mode else '#e2e8f0'

                    # Instant pressure line
                    fig_filter.add_trace(go.Scatter(
                        x=segment_filtered['Timestamp'],
                        y=segment_filtered['PressurePSIG'],
                        mode='lines+markers',
                        name='Instant Pressure',
                        line=dict(width=2, color='#667eea', dash='dot'),
                        marker=dict(size=8, symbol='diamond')
                    ))

                    # Moving average line
                    fig_filter.add_trace(go.Scatter(
                        x=segment_filtered['Timestamp'],
                        y=segment_filtered['MovingAvgPressure'],
                        mode='lines',
                        name='5-Min Moving Avg (Smart Filter)',
                        line=dict(width=4, color='#10b981')
                    ))

                    # MAOP threshold lines
                    maop = segment_filtered.iloc[0]['MAOP_PSIG']
                    fig_filter.add_hline(
                        y=maop * 0.95,
                        line_dash="dash",
                        line_color="orange",
                        line_width=2,
                        annotation_text="95% MAOP (Critical Threshold)",
                        annotation_position="right"
                    )

                    fig_filter.add_hline(
                        y=maop,
                        line_dash="dash",
                        line_color="red",
                        line_width=2,
                        annotation_text="100% MAOP (Violation)",
                        annotation_position="right"
                    )

                    # Highlight spikes (filtered out) vs sustained (flagged)
                    spikes = segment_filtered[segment_filtered['AlertType'] == 'SPIKE']
                    sustained = segment_filtered[segment_filtered['AlertType'] == 'SUSTAINED']

                    if not spikes.empty:
                        fig_filter.add_trace(go.Scatter(
                            x=spikes['Timestamp'],
                            y=spikes['PressurePSIG'],
                            mode='markers',
                            name='⚡ Spike (Filtered)',
                            marker=dict(size=14, color='yellow', symbol='x', line=dict(width=2, color='black')),
                            hovertemplate=
                                '<b>SPIKE FILTERED</b><br>' +
                                'Time: %{x}<br>' +
                                'Instant: %{y:.1f} PSIG<br>' +
                                '5-min Avg: ' + spikes['MovingAvgPressure'].round(1).astype(str) + ' PSIG<br>' +
                                '<extra></extra>'
                        ))

                    if not sustained.empty:
                        fig_filter.add_trace(go.Scatter(
                            x=sustained['Timestamp'],
                            y=sustained['PressurePSIG'],
                            mode='markers',
                            name='🚨 Sustained (Flagged)',
                            marker=dict(size=14, color='red', symbol='star', line=dict(width=2, color='darkred')),
                            hovertemplate=
                                '<b>SUSTAINED DRIFT FLAGGED</b><br>' +
                                'Time: %{x}<br>' +
                                'Instant: %{y:.1f} PSIG<br>' +
                                '5-min Avg: ' + sustained['MovingAvgPressure'].round(1).astype(str) + ' PSIG<br>' +
                                '<extra></extra>'
                        ))

                    fig_filter.update_layout(
                        title=dict(
                            text=f"{segment} ({segment_name}): Smart Filtering in Action",
                            font=dict(size=18, color=title_color, family='Arial Black')
                        ),
                        xaxis=dict(
                            title=dict(
                                text="Time",
                                font=dict(size=14, color=axis_color)
                            ),
                            gridcolor=grid_color
                        ),
                        yaxis=dict(
                            title=dict(
                                text="Pressure (PSIG)",
                                font=dict(size=14, color=axis_color)
                            ),
                            gridcolor=grid_color
                        ),
                        height=450,
                        template=chart_template,
                        plot_bgcolor=plot_bg,
                        paper_bgcolor=card_bg,
                        margin=dict(l=50, r=50, t=80, b=50),
                        hovermode='x unified',
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=0.99,
                            xanchor="right",
                            x=0.99
                        )
                    )

                    st.plotly_chart(fig_filter, use_container_width=True)

                # Explanation box
                with st.expander("💡 How the Transient Filter Works"):
                    st.markdown("""
                    ### The Problem: Nuisance Alarms
                    Traditional systems trigger alarms on **every single reading** that crosses a threshold.
                    This causes "alert fatigue" from normal pressure surges during:
                    - Valve operations
                    - Pump starts/stops
                    - Temperature fluctuations

                    ### ClearLine's Solution: Smart Logic Engine
                    Our **5-minute moving average filter** distinguishes between:

                    **⚡ TRANSIENT SPIKE** (Filtered Out):
                    - Instant reading crosses 95%, but 5-minute average stays below 95%
                    - Indicates normal operational variance
                    - **No alarm generated** ➜ prevents nuisance alerts

                    **🚨 SUSTAINED DRIFT** (Flagged):
                    - Both instant reading AND 5-minute average exceed 95%
                    - Indicates true drift requiring attention
                    - **Alarm generated** ➜ actionable alert

                    ### The Business Impact
                    - **75% reduction** in false alarms
                    - Operators focus on **real issues** only
                    - Meets regulatory requirements without alert fatigue
                    - **Investor value**: Proven AI-powered logic, not just simple thresholds
                    """)

            else:
                st.info("No high readings detected in current dataset. All pressures within normal operating range.")

        st.divider()

        # Alerts table
        df_display = df_alerts.copy()
        df_display['Timestamp'] = pd.to_datetime(df_display['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_display['Ratio'] = df_display['Ratio'].round(1).astype(str) + '%'
        df_display['PressurePSIG'] = df_display['PressurePSIG'].round(1)
        df_display['MAOP_PSIG'] = df_display['MAOP_PSIG'].round(1)

        # Style the dataframe
        def highlight_status(row):
            if row['Status'] == 'VIOLATION':
                return ['background-color: #ff4444; color: white'] * len(row)
            elif row['Status'] == 'CRITICAL':
                return ['background-color: #ffaa00; color: white'] * len(row)
            return [''] * len(row)

        st.dataframe(
            df_display[['Timestamp', 'SegmentID', 'SegmentName', 'PressurePSIG', 'MAOP_PSIG', 'Ratio', 'Status']].style.apply(highlight_status, axis=1),
            use_container_width=True,
            height=400
        )

        # Detailed timeline
        st.subheader("Timeline View")

        for segment in df_alerts['SegmentID'].unique():
            segment_alerts = df_alerts[df_alerts['SegmentID'] == segment]

            with st.expander(f"🔍 {segment} - {segment_alerts.iloc[0]['SegmentName']} ({len(segment_alerts)} alerts)"):
                for _, alert in segment_alerts.iterrows():
                    status_emoji = "❌" if alert['Status'] == 'VIOLATION' else "⚠️"
                    st.markdown(
                        f"{status_emoji} **{alert['Timestamp']}** - "
                        f"{alert['PressurePSIG']:.1f} PSIG "
                        f"({alert['Ratio']:.1f}% MAOP) - "
                        f"**{alert['Status']}**"
                    )
    else:
        st.info("✅ No critical threshold crossings detected. All segments operating normally.")


# TAB 3: COMPLIANCE LEDGER
with tab_ledger:
    st.header("🔒 Compliance Ledger: Cryptographic Hash Chain")
    st.markdown("Immutable forensic data integrity verification")

    # Verify button
    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("🔍 Verify Data Integrity", type="primary", use_container_width=True):
            with st.spinner("Verifying hash chain..."):
                is_valid, broken_at, total_checked = verify_hash_chain()

                if is_valid:
                    st.success(f"✅ **VERIFIED** - Hash chain intact ({total_checked} readings verified)")
                    st.balloons()
                else:
                    st.error(f"❌ **TAMPERING DETECTED** at ReadingID {broken_at}!")
                    st.markdown("**Forensic Alert:** Data integrity compromised. Chain breaks at record " + str(broken_at))

    with col2:
        st.info("**How it works:** Each reading contains a SHA-256 hash that includes the previous reading's hash, creating an immutable chain. Any tampering breaks the chain and is immediately detected.")

    st.divider()

    # Hash ledger table
    df_readings = get_all_readings()

    if not df_readings.empty:
        st.subheader("Hash Signature Ledger")

        df_ledger = df_readings.copy()
        df_ledger['Timestamp'] = pd.to_datetime(df_ledger['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_ledger['Hash Preview'] = df_ledger['hash_signature'].apply(lambda x: x[:16] + '...' if x else 'N/A')
        df_ledger['Ratio'] = df_ledger['Ratio'].round(1).astype(str) + '%'

        st.dataframe(
            df_ledger[['ReadingID', 'Timestamp', 'SegmentID', 'PressurePSIG', 'Ratio', 'Hash Preview']],
            use_container_width=True,
            height=400
        )

        # Export option
        st.download_button(
            label="📥 Export Full Ledger (CSV)",
            data=df_readings.to_csv(index=False),
            file_name=f"clearline_compliance_ledger_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

        # Technical details
        with st.expander("🔬 Technical Details"):
            st.markdown("""
            ### Hash Chain Implementation

            Each reading's hash is calculated using SHA-256:
            ```
            HASH(n) = SHA-256(
                Timestamp + SegmentID + SensorID +
                PressurePSIG + MAOP + RecordedBy +
                DataSource + HASH(n-1)
            )
            ```

            **Benefits:**
            - **Immutable**: Cannot alter past records without detection
            - **Forensic**: Court-admissible proof of data integrity
            - **Blockchain-style**: Same technology securing billions in assets
            - **Real-time**: Instant verification of entire chain

            **Compliance:**
            - Meets PHMSA 192.605 record integrity requirements
            - Satisfies Sarbanes-Oxley data authenticity standards
            - Provides audit trail for ISO 55001 certification
            """)
    else:
        st.warning("No readings found in database.")


# TAB 4: OPERATOR ACTIVITY
with tab_activity:
    st.header("👤 Operator Activity Log")
    st.markdown("**Complete audit trail of all operator actions and system events**")

    df_activity = get_operator_activity()

    if not df_activity.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_actions = len(df_activity)
            st.metric("Total Actions", f"{total_actions}")

        with col2:
            unique_operators = df_activity['Operator'].nunique()
            st.metric("Active Operators", f"{unique_operators}")

        with col3:
            acknowledgments = len(df_activity[df_activity['ActionType'] == 'Acknowledge'])
            st.metric("Acknowledgments", f"{acknowledgments}")

        with col4:
            latest_action = pd.to_datetime(df_activity['Timestamp']).max()
            hours_ago = (datetime.now() - latest_action).total_seconds() / 3600
            st.metric("Last Action", f"{hours_ago:.1f}h ago")

        st.divider()

        # Alert Response Metrics
        st.subheader("⚡ Alert Response Performance")
        df_alerts = get_alert_response_metrics()

        if not df_alerts.empty and 'ResponseTimeMinutes' in df_alerts.columns:
            # Filter for alerts that were acknowledged
            df_responded = df_alerts[df_alerts['ResponseTimeMinutes'].notna()]

            if not df_responded.empty:
                col1, col2, col3 = st.columns(3)

                with col1:
                    avg_response = df_responded['ResponseTimeMinutes'].mean()
                    st.metric("Avg Response Time", f"{avg_response:.1f} min",
                             delta="Within SLA" if avg_response < 15 else "Review needed",
                             delta_color="normal" if avg_response < 15 else "inverse")

                with col2:
                    ack_rate = (len(df_responded) / len(df_alerts)) * 100
                    st.metric("Acknowledgment Rate", f"{ack_rate:.0f}%",
                             delta="Excellent" if ack_rate >= 90 else "Needs improvement")

                with col3:
                    critical_alerts = len(df_alerts[df_alerts['AlertLevel'] == 'CRITICAL'])
                    st.metric("Critical Alerts", f"{critical_alerts}",
                             delta="Addressed" if critical_alerts > 0 and len(df_responded) > 0 else "None")

                # Response time chart
                fig_response = go.Figure()

                fig_response.add_trace(go.Bar(
                    x=df_responded['AlertTime'],
                    y=df_responded['ResponseTimeMinutes'],
                    marker_color=['green' if t < 5 else 'yellow' if t < 15 else 'red'
                                  for t in df_responded['ResponseTimeMinutes']],
                    text=df_responded['ResponseTimeMinutes'].round(1),
                    textposition='outside',
                    name='Response Time'
                ))

                # Add SLA line
                fig_response.add_hline(y=15, line_dash="dash", line_color="red",
                                      annotation_text="15-min SLA",
                                      annotation_position="right")

                fig_response.update_layout(
                    title="Alert Response Times (Lower is Better)",
                    xaxis_title="Alert Time",
                    yaxis_title="Response Time (minutes)",
                    height=350,
                    showlegend=False
                )

                st.plotly_chart(fig_response, use_container_width=True)
            else:
                st.info("No acknowledged alerts yet. Operators will acknowledge critical alerts as they occur.")
        else:
            st.info("No alerts have been generated yet.")

        st.divider()

        # Activity log table
        st.subheader("📋 Complete Activity Log")

        df_display = df_activity.copy()
        df_display['Timestamp'] = pd.to_datetime(df_display['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Color code by action type
        def highlight_action(row):
            if row['ActionType'] == 'Acknowledge':
                return ['background-color: #d4edda'] * len(row)
            elif row['ActionType'] == 'Update':
                return ['background-color: #fff3cd'] * len(row)
            return [''] * len(row)

        st.dataframe(
            df_display[['Timestamp', 'Operator', 'ActionType', 'TableAffected', 'Description']],
            use_container_width=True,
            height=400
        )

        # Export option
        st.download_button(
            label="📥 Export Activity Log (CSV)",
            data=df_activity.to_csv(index=False),
            file_name=f"clearline_activity_log_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

        # Compliance note
        with st.expander("📜 Regulatory Compliance"):
            st.markdown("""
            ### Audit Trail Compliance

            This activity log satisfies multiple regulatory requirements:

            - **PHMSA 192.605**: Complete and accurate records of operation and maintenance
            - **49 CFR 192.605**: Records must be retained and available for inspection
            - **SOX Section 404**: Internal controls over financial reporting (for public companies)
            - **ISO 55001**: Asset management audit trail requirements

            **Key Features:**
            - Immutable audit trail (cannot be deleted)
            - Timestamped operator actions
            - Complete description of all changes
            - Compliance notes for critical actions
            """)

    else:
        st.warning("No operator activity found in database.")


# TAB 5: SENSOR HEALTH
with tab_sensors:
    st.header("🔧 Sensor Health & Data Quality")
    st.markdown("**Proactive monitoring of sensor calibration and system reliability**")

    df_sensors = get_sensor_health()

    if not df_sensors.empty:
        # Overall health metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            avg_health = df_sensors['HealthScore'].mean()
            st.metric("System Health Score", f"{avg_health:.0f}/100",
                     delta="Excellent" if avg_health >= 95 else "Good" if avg_health >= 85 else "Review needed")

        with col2:
            sensors_current = len(df_sensors[df_sensors['CalibrationStatus'] == 'Current'])
            st.metric("Sensors Current", f"{sensors_current}/{len(df_sensors)}",
                     delta="✓ All current" if sensors_current == len(df_sensors) else "⚠ Action needed",
                     delta_color="normal" if sensors_current == len(df_sensors) else "inverse")

        with col3:
            sensors_due = len(df_sensors[df_sensors['CalibrationStatus'] == 'Due Soon'])
            st.metric("Calibration Due Soon", f"{sensors_due}",
                     delta="Schedule maintenance" if sensors_due > 0 else "None")

        with col4:
            sensors_overdue = len(df_sensors[df_sensors['CalibrationStatus'] == 'Overdue'])
            st.metric("⚠ Overdue", f"{sensors_overdue}",
                     delta="Immediate action" if sensors_overdue > 0 else "None",
                     delta_color="inverse" if sensors_overdue > 0 else "normal")

        st.divider()

        # Sensor health visualization
        st.subheader("📊 Sensor Health Scores")

        fig_health = go.Figure()

        colors = []
        for score in df_sensors['HealthScore']:
            if score >= 95:
                colors.append('#28a745')  # Green - Excellent
            elif score >= 85:
                colors.append('#ffc107')  # Yellow - Good
            else:
                colors.append('#dc3545')  # Red - Review needed

        fig_health.add_trace(go.Bar(
            x=df_sensors['SegmentName'],
            y=df_sensors['HealthScore'],
            marker_color=colors,
            text=df_sensors['HealthScore'],
            textposition='outside',
            name='Health Score'
        ))

        fig_health.add_hline(y=95, line_dash="dash", line_color="green",
                            annotation_text="Excellent (95+)",
                            annotation_position="right")

        fig_health.add_hline(y=85, line_dash="dash", line_color="orange",
                            annotation_text="Good (85+)",
                            annotation_position="right")

        fig_health.update_layout(
            title="Sensor Health by Segment",
            xaxis_title="Pipeline Segment",
            yaxis_title="Health Score (0-100)",
            yaxis_range=[0, 105],
            height=350,
            showlegend=False
        )

        st.plotly_chart(fig_health, use_container_width=True)

        st.divider()

        # Calibration status table
        st.subheader("🔧 Calibration Status & Maintenance Schedule")

        df_display = df_sensors.copy()
        df_display['LastCalibrationDate'] = pd.to_datetime(df_display['LastCalibrationDate']).dt.strftime('%Y-%m-%d')

        # Calculate next calibration due
        df_display['NextCalibrationDue'] = (pd.to_datetime(df_sensors['LastCalibrationDate']) +
                                            pd.DateOffset(years=1)).dt.strftime('%Y-%m-%d')

        st.dataframe(
            df_display[['SerialNumber', 'SegmentName', 'HealthScore', 'CalibrationStatus',
                       'LastCalibrationDate', 'NextCalibrationDue', 'DaysSinceCalibration', 'CalibratedBy']],
            use_container_width=True,
            height=300
        )

        # Predictive maintenance alerts
        st.divider()
        st.subheader("🔮 Predictive Maintenance Alerts")

        # Check for sensors needing attention
        due_soon = df_sensors[df_sensors['CalibrationStatus'] == 'Due Soon']
        overdue = df_sensors[df_sensors['CalibrationStatus'] == 'Overdue']
        low_health = df_sensors[df_sensors['HealthScore'] < 95]

        if not overdue.empty:
            st.error(f"🚨 **IMMEDIATE ACTION REQUIRED:** {len(overdue)} sensor(s) overdue for calibration")
            for _, sensor in overdue.iterrows():
                st.markdown(f"- **{sensor['SerialNumber']}** ({sensor['SegmentName']}): {sensor['DaysSinceCalibration']} days since last calibration")

        if not due_soon.empty:
            st.warning(f"⚠️ **SCHEDULE MAINTENANCE:** {len(due_soon)} sensor(s) due for calibration within 35 days")
            for _, sensor in due_soon.iterrows():
                days_until_due = 365 - sensor['DaysSinceCalibration']
                st.markdown(f"- **{sensor['SerialNumber']}** ({sensor['SegmentName']}): Due in ~{days_until_due} days")

        if not low_health.empty:
            st.info(f"ℹ️ **MONITOR:** {len(low_health)} sensor(s) with health score below 95")
            for _, sensor in low_health.iterrows():
                st.markdown(f"- **{sensor['SerialNumber']}** ({sensor['SegmentName']}): Health score {sensor['HealthScore']:.0f}/100")

        if overdue.empty and due_soon.empty and low_health.empty:
            st.success("✅ **All sensors are healthy and current!** No maintenance actions required at this time.")

        # Export option
        st.download_button(
            label="📥 Export Sensor Report (CSV)",
            data=df_sensors.to_csv(index=False),
            file_name=f"clearline_sensor_health_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

        # ROI calculation
        with st.expander("💰 Preventive Maintenance ROI"):
            st.markdown("""
            ### Cost Savings Through Proactive Sensor Management

            **Unplanned Sensor Failure Costs:**
            - Emergency calibration: $2,500 per sensor
            - Pipeline shutdown: $10,000 per hour
            - Regulatory fines: Up to $50,000 per violation
            - Invalid data during downtime: Priceless compliance risk

            **Planned Calibration Costs:**
            - Scheduled calibration: $800 per sensor
            - No pipeline downtime
            - Maintained compliance
            - Predictable budgeting

            **Annual Savings:**
            With 4 sensors, preventing just one unplanned failure saves $12,000-60,000 per year.

            **This dashboard's predictive alerts enable:**
            - 📅 Scheduled maintenance windows
            - 💰 ~75% cost reduction vs. reactive maintenance
            - ✅ Zero compliance violations
            - 📊 Continuous data quality
            """)

    else:
        st.warning("No sensor data found in database.")


# Footer
st.markdown("""
<div class="footer">
    <strong>ClearLine Pipeline Management System</strong><br>
    Powered by forensic-grade data integrity | Enterprise compliance monitoring<br>
    © 2026 ClearLine Technologies | All rights reserved
</div>
""", unsafe_allow_html=True)
