"""
ClearLine Pipeline Integrity Management System
Professional-grade SCADA monitoring and compliance platform

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
    page_title="ClearLine Pipeline Integrity Management",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'dark_mode' not in st.session_state:
    st.session_state.dark_mode = False
if 'demo_mode' not in st.session_state:
    st.session_state.demo_mode = False
if 'demo_step' not in st.session_state:
    st.session_state.demo_step = 0
if 'demo_timeline_index' not in st.session_state:
    st.session_state.demo_timeline_index = 0
if 'simulator_active' not in st.session_state:
    st.session_state.simulator_active = False
if 'simulator_interval' not in st.session_state:
    st.session_state.simulator_interval = 5
if 'show_help' not in st.session_state:
    st.session_state.show_help = False
if 'onboarding_complete' not in st.session_state:
    st.session_state.onboarding_complete = False
if 'selected_segments' not in st.session_state:
    st.session_state.selected_segments = []
if 'time_range' not in st.session_state:
    st.session_state.time_range = '24h'
if 'comparison_mode' not in st.session_state:
    st.session_state.comparison_mode = False

# iOS-Inspired Professional Color Scheme
if st.session_state.dark_mode:
    # Dark Mode - iOS inspired
    bg_color = "#000000"
    header_bg = "#1c1c1e"
    text_color = "#ffffff"
    card_bg = "#1c1c1e"
    plot_bg = "#1c1c1e"
    metric_color = "#ffffff"
    label_color = "#98989d"
    border_color = "#38383a"
    accent_color = "#0a84ff"  # iOS blue
    success_color = "#32d74b"  # iOS green
    warning_color = "#ff9f0a"  # iOS orange
    danger_color = "#ff453a"  # iOS red
    secondary_bg = "#2c2c2e"
else:
    # Light Mode - iOS inspired
    bg_color = "#f2f2f7"
    header_bg = "#ffffff"
    text_color = "#000000"
    card_bg = "#ffffff"
    plot_bg = "#ffffff"
    metric_color = "#000000"
    label_color = "#8e8e93"
    border_color = "#d1d1d6"
    accent_color = "#007aff"  # iOS blue
    success_color = "#34c759"  # iOS green
    warning_color = "#ff9500"  # iOS orange
    danger_color = "#ff3b30"  # iOS red
    secondary_bg = "#f2f2f7"

# World-Class Design System
# Design Principles Applied:
# 1. Dieter Rams - "Less but Better": Minimal design, honest, understandable
# 2. Apple/Jony Ive - 8pt Grid System, depth through layers, clarity
# 3. Swiss Design/Vignelli - Mathematical proportions, typography hierarchy, whitespace
# 4. Don Norman - Visibility, feedback, affordances, consistency
# 5. Edward Tufte - Data-ink ratio, clear information architecture
st.markdown(f"""
<style>
    /*
    DESIGN PHILOSOPHY:
    - Form follows function (Bauhaus/Swiss Design)
    - Progressive disclosure (Apple)
    - Clear visual hierarchy (Vignelli)
    - Immediate feedback (Norman)
    - As little design as possible (Rams)
    */

    /* Typography - Helvetica Neue / Inter (Swiss Design) */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* 8pt Grid System (Apple Design) */
    :root {{
        --space-xs: 4px;
        --space-sm: 8px;
        --space-md: 16px;
        --space-lg: 24px;
        --space-xl: 32px;
        --space-xxl: 48px;
    }}

    /* Remove default Streamlit padding */
    .block-container {{
        padding-top: var(--space-md) !important;
        padding-bottom: 0rem !important;
        max-width: 100% !important;
    }}

    /* Main app - Clean Background (Rams: Less but Better) */
    .stApp {{
        background: {bg_color};
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', 'Segoe UI', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        line-height: 1.6;
    }}

    /* Main container - Mathematical Proportions (Vignelli) */
    .main {{
        background: {bg_color};
        padding: 0;
        max-width: 1400px;
        margin: 0 auto;
    }}

    /* Sidebar - Professional style */
    [data-testid="stSidebar"] {{
        background: {card_bg};
        border-right: 1px solid {border_color};
        padding: 0 !important;
    }}

    [data-testid="stSidebar"] > div:first-child {{
        padding: 0 !important;
    }}

    [data-testid="stSidebar"] .block-container {{
        padding-top: 0 !important;
        padding-left: 1.25rem !important;
        padding-right: 1.25rem !important;
    }}

    /* iOS-style Header */
    .main-header {{
        background: {header_bg};
        padding: 1.25rem 2rem;
        margin: 0;
        border-bottom: 0.5px solid {border_color};
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
    }}

    .header-title {{
        color: {text_color};
        font-weight: 600;
        font-size: 1.75rem;
        margin: 0;
        letter-spacing: -0.02em;
        line-height: 1.2;
    }}

    .header-subtitle {{
        color: {label_color};
        font-size: 0.8125rem;
        font-weight: 400;
        margin: 0.25rem 0 0 0;
        letter-spacing: -0.01em;
    }}

    /* iOS-style Status Badge */
    .status-badge {{
        display: inline-flex;
        align-items: center;
        padding: 0.375rem 0.75rem;
        border-radius: 20px;
        font-size: 0.6875rem;
        font-weight: 600;
        letter-spacing: 0.02em;
        text-transform: uppercase;
    }}

    .status-operational {{
        background: {success_color}20;
        color: {success_color};
        border: 1px solid {success_color}40;
    }}

    .status-warning {{
        background: {warning_color}20;
        color: {warning_color};
        border: 1px solid {warning_color}40;
    }}

    .status-critical {{
        background: {danger_color}20;
        color: {danger_color};
        border: 1px solid {danger_color}40;
    }}

    /* Metrics - Rams' Honest Design + Clear Hierarchy */
    div[data-testid="stMetricValue"] {{
        font-size: 2.25rem;
        font-weight: 700;
        color: {text_color};
        letter-spacing: -0.03em;
        line-height: 1.1;
        margin: var(--space-sm) 0;
    }}

    /* Labels - Swiss Design Typography */
    div[data-testid="stMetricLabel"] {{
        font-size: 0.6875rem;
        font-weight: 600;
        color: {label_color};
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: var(--space-xs);
    }}

    /* Delta - Subtle Secondary Information */
    div[data-testid="stMetricDelta"] {{
        font-size: 0.8125rem;
        font-weight: 500;
        margin-top: var(--space-xs);
    }}

    /* Cards - Swiss Design Precision + Norman's Affordance */
    [data-testid="metric-container"] {{
        background: {card_bg};
        padding: var(--space-lg);
        border-radius: 12px;
        border: 1px solid {border_color};
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04), 0 1px 2px rgba(0, 0, 0, 0.06);
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }}

    /* Subtle Interaction Feedback (Norman: Visibility & Feedback) */
    [data-testid="metric-container"]:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08), 0 2px 4px rgba(0, 0, 0, 0.06);
        border-color: {accent_color}30;
    }}

    /* Progressive Disclosure (Apple: Depth through Layers) */
    [data-testid="metric-container"]::after {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        background: {accent_color};
        opacity: 0;
        transition: opacity 0.2s ease;
    }}

    [data-testid="metric-container"]:hover::after {{
        opacity: 0.6;
    }}

    /* Enhanced Tabs with Gradient Accent */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        background-color: {secondary_bg};
        padding: 8px;
        border-radius: 12px;
        border: 1px solid {border_color};
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
        overflow-x: auto !important;
        overflow-y: hidden;
        white-space: nowrap;
        display: flex !important;
        flex-wrap: nowrap !important;
        scroll-behavior: smooth;
        -webkit-overflow-scrolling: touch;
        position: relative;
    }}

    .stTabs [data-baseweb="tab-list"]::before {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        background: linear-gradient(90deg, {accent_color} 0%, {'#0051d5' if not st.session_state.dark_mode else '#3a9aff'} 100%);
    }}

    /* Custom scrollbar for tabs */
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {{
        height: 8px;
        background: transparent;
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {{
        background: {secondary_bg};
        border-radius: 10px;
        margin: 0 4px;
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {{
        background: {accent_color}60;
        border-radius: 10px;
        border: 2px solid {secondary_bg};
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {{
        background: {accent_color}90;
    }}

    .stTabs [data-baseweb="tab"] {{
        height: 40px;
        background-color: transparent;
        border-radius: 8px;
        color: {label_color};
        font-weight: 500;
        font-size: 0.8125rem;
        padding: 0 1.25rem;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        border: none;
        letter-spacing: -0.01em;
        flex-shrink: 0;
        min-width: fit-content;
        position: relative;
    }}

    .stTabs [data-baseweb="tab"]:hover {{
        background-color: {card_bg};
        color: {text_color};
        transform: translateY(-1px);
    }}

    .stTabs [aria-selected="true"] {{
        background: linear-gradient(135deg, {card_bg} 0%, {accent_color}10 100%);
        color: {accent_color};
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1);
        font-weight: 600;
        border: 1px solid {accent_color}40;
    }}

    .stTabs [aria-selected="true"]::before {{
        content: '';
        position: absolute;
        bottom: 0;
        left: 50%;
        transform: translateX(-50%);
        width: 30%;
        height: 2px;
        background: {accent_color};
    }}

    /* Buttons - Apple's Tactile Design + Swiss Functionality */
    .stButton > button {{
        background: {card_bg};
        color: {text_color};
        border: 1px solid {border_color};
        border-radius: 8px;
        padding: var(--space-sm) var(--space-md);
        font-weight: 500;
        font-size: 0.875rem;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        letter-spacing: -0.01em;
        width: 100%;
        text-align: left;
        cursor: pointer;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
    }}

    /* Hover Feedback (Norman: Clear Affordances) */
    .stButton > button:hover {{
        background: {secondary_bg};
        border-color: {accent_color}50;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08);
        transform: translateY(-1px);
    }}

    /* Active State (Immediate Feedback) */
    .stButton > button:active {{
        transform: scale(0.98) translateY(0);
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
    }}

    /* Primary Actions - Clear Hierarchy (Rams: Honest Design) */
    .stButton > button[kind="primary"] {{
        background: {accent_color};
        color: white;
        border: 1px solid {accent_color};
        font-weight: 600;
        box-shadow: 0 2px 4px {accent_color}30;
    }}

    .stButton > button[kind="primary"]:hover {{
        background: {accent_color};
        opacity: 0.92;
        box-shadow: 0 4px 12px {accent_color}40;
        transform: translateY(-1px);
    }}

    /* Download button */
    .stDownloadButton > button {{
        background: {success_color};
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        font-size: 0.8125rem;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 2px 8px {success_color}30;
    }}

    .stDownloadButton > button:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 12px {success_color}40;
    }}

    /* Data Tables - Swiss Design Grid System */
    .dataframe {{
        border: none !important;
        border-radius: 12px;
        background-color: {card_bg};
        font-size: 0.875rem;
        letter-spacing: -0.01em;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        overflow: hidden;
    }}

    /* Table Headers - Clear Hierarchy (Vignelli: Typography) */
    .dataframe th {{
        background-color: {secondary_bg} !important;
        color: {label_color} !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.6875rem !important;
        letter-spacing: 0.08em;
        padding: var(--space-md) var(--space-md) !important;
        border-bottom: 2px solid {border_color} !important;
        text-align: left;
    }}

    /* Table Cells - Breathing Room (Swiss Design: Whitespace) */
    .dataframe td {{
        padding: var(--space-md) var(--space-md) !important;
        border-bottom: 1px solid {border_color} !important;
        color: {text_color} !important;
        line-height: 1.5;
    }}

    /* Zebra Striping for Readability (Norman: Usability) */
    .dataframe tbody tr:nth-child(even) {{
        background-color: {secondary_bg}40;
    }}

    .dataframe tbody tr:hover {{
        background-color: {accent_color}08;
        transition: background-color 0.15s ease;
    }}

    /* Alerts - Norman's Feedback Principles */
    .stAlert {{
        border-radius: 12px;
        border: 1px solid {border_color};
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
        font-size: 0.875rem;
        letter-spacing: -0.01em;
        padding: var(--space-md) var(--space-lg);
        line-height: 1.5;
    }}

    /* Status Indicators - Clear Visual Language (Rams: Understandable) */
    .status-indicator {{
        display: inline-flex;
        align-items: center;
        gap: var(--space-xs);
        padding: var(--space-xs) var(--space-md);
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }}

    /* Charts - Data Visualization Excellence (Tufte: Data-Ink Ratio) */
    .js-plotly-plot {{
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04), 0 1px 2px rgba(0, 0, 0, 0.06);
        background: {card_bg};
        padding: var(--space-md);
        border: 1px solid {border_color};
        margin: var(--space-lg) 0;
    }}

    /* Chart Container - Proper Breathing Room */
    .stPlotlyChart {{
        padding: var(--space-md) 0;
    }}

    /* Typography Hierarchy - Swiss Design + Rams' Clarity */
    h1, h2, h3, h4, h5, h6 {{
        color: {text_color} !important;
        font-weight: 600;
        letter-spacing: -0.02em;
        line-height: 1.2;
        margin-top: 0;
    }}

    h1 {{
        font-size: 2.25rem;
        font-weight: 700;
        letter-spacing: -0.03em;
        margin-bottom: var(--space-md);
    }}

    /* Visual Hierarchy through Whitespace (Vignelli) */
    h2 {{
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: var(--space-lg);
        padding-bottom: var(--space-md);
        border-bottom: 1px solid {border_color};
    }}

    h3 {{
        font-size: 1.125rem;
        font-weight: 600;
        margin-bottom: var(--space-md);
        color: {text_color} !important;
    }}

    /* Body Text - Optimal Readability (Norman: Usability) */
    p {{
        line-height: 1.6;
        margin-bottom: var(--space-md);
        color: {text_color};
    }}

    /* Info boxes */
    .info-box {{
        background: {card_bg};
        border-left: 3px solid {accent_color};
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }}

    /* Fix all text colors in dark mode */
    p, span, div, label {{
        color: {text_color};
    }}

    /* Streamlit specific text elements */
    .stMarkdown, .stMarkdown p, .stMarkdown span {{
        color: {text_color} !important;
    }}

    /* Dividers - Subtle Separation (Minimalist) */
    hr {{
        margin: var(--space-xl) 0;
        border: none;
        height: 1px;
        background: {border_color};
        opacity: 0.6;
    }}

    /* Spacing System - 8pt Grid (Apple Design) */
    .element-container {{
        margin-bottom: var(--space-md);
    }}

    /* Section Spacing - Clear Visual Rhythm (Vignelli) */
    [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] {{
        gap: var(--space-lg);
    }}

    /* Professional Select boxes - Swiss Design Clarity */
    .stSelectbox > div > div {{
        border-radius: 8px;
        border: 1px solid {border_color};
        background: {card_bg};
        font-size: 0.875rem;
        padding: 0.625rem;
        transition: all 0.15s ease;
        color: {text_color} !important;
    }}

    .stSelectbox label {{
        color: {text_color} !important;
    }}

    /* Target the selected value display - Multiple selectors for coverage */
    .stSelectbox [data-baseweb="select"] {{
        color: {text_color} !important;
    }}

    .stSelectbox [data-baseweb="select"] > div {{
        color: {text_color} !important;
    }}

    .stSelectbox [data-baseweb="select"] span {{
        color: {text_color} !important;
    }}

    .stSelectbox [data-baseweb="select"] * {{
        color: {text_color} !important;
    }}

    .stSelectbox div[role="button"] {{
        color: {text_color} !important;
    }}

    .stSelectbox div[role="button"] > div {{
        color: {text_color} !important;
    }}

    .stSelectbox div[role="button"] span {{
        color: {text_color} !important;
    }}

    .stSelectbox div[role="button"] * {{
        color: {text_color} !important;
    }}

    /* Target input element if present */
    .stSelectbox input {{
        color: {text_color} !important;
    }}

    /* Force all text inside selectbox to be visible */
    .stSelectbox * {{
        color: {text_color} !important;
    }}

    /* SUPER AGGRESSIVE: Target Streamlit's select value text */
    [class*="singleValue"],
    [class*="singleValue"] *,
    [data-baseweb="select"] [class*="singleValue"],
    [data-baseweb="select"] [class*="singleValue"] *,
    div[data-baseweb="select"] > div > div,
    div[data-baseweb="select"] > div > div > div {{
        color: {text_color} !important;
        opacity: 1 !important;
        visibility: visible !important;
        display: block !important;
    }}

    /* Force text to be visible in select components */
    [role="combobox"] *,
    [role="button"] div {{
        color: {text_color} !important;
        opacity: 1 !important;
    }}

    /* Override any inline styles */
    .stSelectbox div[style] {{
        color: {text_color} !important;
    }}

    /* Hover and Focus States */
    .stSelectbox > div > div:hover {{
        border-color: {accent_color}60;
        box-shadow: 0 1px 4px rgba(0, 0, 0, 0.06);
    }}

    .stSelectbox > div > div:focus-within {{
        border-color: {accent_color};
        box-shadow: 0 0 0 3px {accent_color}20;
    }}

    /* Multiselect styling */
    .stMultiSelect > div > div {{
        border-radius: 8px;
        border: 1px solid {border_color};
        background: {card_bg};
        font-size: 0.875rem;
        transition: all 0.15s ease;
    }}

    .stMultiSelect > div > div:hover {{
        border-color: {accent_color}60;
    }}

    .stMultiSelect > div > div:focus-within {{
        border-color: {accent_color};
        box-shadow: 0 0 0 3px {accent_color}20;
    }}

    /* Multiselect tags */
    .stMultiSelect span[data-baseweb="tag"] {{
        background: {accent_color}20 !important;
        border: 1px solid {accent_color}40 !important;
        border-radius: 6px !important;
        color: {accent_color} !important;
        font-weight: 500 !important;
        font-size: 0.75rem !important;
        padding: 0.25rem 0.5rem !important;
    }}

    /* Checkbox styling */
    .stCheckbox {{
        font-size: 0.875rem;
    }}

    .stCheckbox > label {{
        color: {text_color};
        font-weight: 500;
    }}

    /* Expander */
    .streamlit-expanderHeader {{
        background-color: {secondary_bg};
        border-radius: 10px;
        font-weight: 500;
        border: 0.5px solid {border_color};
    }}

    /* Remove top padding entirely */
    .main .block-container {{
        padding-top: 0rem !important;
    }}

    /* Custom control panel */
    .control-panel {{
        background: {card_bg};
        padding: 1rem;
        border-radius: 12px;
        border: 0.5px solid {border_color};
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }}

    /* Help tooltip */
    .help-icon {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 20px;
        height: 20px;
        border-radius: 50%;
        background: {accent_color}20;
        color: {accent_color};
        font-size: 0.75rem;
        font-weight: 600;
        cursor: pointer;
        margin-left: 0.5rem;
    }}
</style>
""", unsafe_allow_html=True)

# Sidebar - Clean Professional Design (Dieter Rams: Less but Better)
with st.sidebar:
    # Header Section - Clean, No Gradient (Swiss Design: Function over Form)
    st.markdown(f"""
    <div style="padding: 1.5rem 0; margin-bottom: 1.5rem; border-bottom: 2px solid {border_color};">
        <h1 style="font-size: 1.75rem; font-weight: 700; margin: 0; color: {text_color};
                   letter-spacing: -0.04em; line-height: 1.1;">
            ClearLine
        </h1>
        <p style="font-size: 0.8125rem; color: {label_color}; margin: 0.5rem 0 0 0;
                  font-weight: 400; line-height: 1.4;">
            Pipeline Integrity Management
        </p>
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-top: 1rem;">
            <div style="width: 6px; height: 6px; border-radius: 50%; background: {success_color};
                       box-shadow: 0 0 8px {success_color}60;"></div>
            <span style="font-size: 0.6875rem; color: {text_color}; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em;">
                LIVE • {datetime.now().strftime('%I:%M:%S %p')}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Settings Section
    st.markdown(f"""
    <div style="margin: 1.5rem 0 0.75rem 0;">
        <h3 style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                   text-transform: uppercase; letter-spacing: 0.08em; margin: 0;">
            Settings
        </h3>
    </div>
    """, unsafe_allow_html=True)

    # Theme toggle - styled button
    theme_label = "☀️ Light Mode" if st.session_state.dark_mode else "🌙 Dark Mode"
    if st.button(theme_label, key="theme_toggle", use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()

    # Time range selector
    st.markdown(f"""
    <div style="margin: 1.5rem 0 0.5rem 0;">
        <label style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                      text-transform: uppercase; letter-spacing: 0.08em; display: block;">
            Time Range
        </label>
    </div>
    """, unsafe_allow_html=True)

    time_options = {
        '1h': 'Last Hour',
        '24h': 'Last 24 Hours',
        '7d': 'Last 7 Days',
        '30d': 'Last 30 Days',
        'all': 'All Time'
    }
    st.session_state.time_range = st.selectbox(
        "Select Time Range",
        options=list(time_options.keys()),
        format_func=lambda x: time_options[x],
        index=1,
        label_visibility="collapsed"
    )

    # Segment filter
    st.markdown(f"""
    <div style="margin: 1.5rem 0 0.5rem 0;">
        <label style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                      text-transform: uppercase; letter-spacing: 0.08em; display: block;">
            Segment Filter
        </label>
    </div>
    """, unsafe_allow_html=True)

    all_segments = ['SEG-01', 'SEG-02', 'SEG-03', 'SEG-04']
    st.session_state.selected_segments = st.multiselect(
        "Select Segments",
        options=all_segments,
        default=all_segments,
        label_visibility="collapsed"
    )

    # Comparison mode
    st.markdown(f"""
    <div style="margin: 1.5rem 0 0.5rem 0;">
        <label style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                      text-transform: uppercase; letter-spacing: 0.08em; display: block;">
            Display Options
        </label>
    </div>
    """, unsafe_allow_html=True)
    st.session_state.comparison_mode = st.checkbox("Comparison View", value=False)

    # Divider
    st.markdown(f"""
    <div style="border-bottom: 1px solid {border_color}; margin: 1.5rem 0;"></div>
    """, unsafe_allow_html=True)

    # Help & Support Section
    st.markdown(f"""
    <div style="margin: 1.5rem 0 0.75rem 0;">
        <h3 style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                   text-transform: uppercase; letter-spacing: 0.08em; margin: 0;">
            Help & Support
        </h3>
    </div>
    """, unsafe_allow_html=True)

    if st.button("Start Onboarding Tour", use_container_width=True):
        st.session_state.show_help = True
        st.session_state.demo_mode = True
        st.session_state.demo_step = 0
        st.rerun()

    if st.button("Documentation", use_container_width=True):
        st.info("Documentation: https://clearline.com/docs")

    # Divider
    st.markdown(f"""
    <div style="border-bottom: 1px solid {border_color}; margin: 1.5rem 0;"></div>
    """, unsafe_allow_html=True)

    # System Status Section
    st.markdown(f"""
    <div style="margin: 1.5rem 0 0.75rem 0;">
        <h3 style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                   text-transform: uppercase; letter-spacing: 0.08em; margin: 0;">
            System Status
        </h3>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="background: {success_color}15; border: 1px solid {success_color}30;
                border-radius: 8px; padding: 0.75rem; margin-top: 0.5rem;">
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.375rem;">
            <div style="width: 8px; height: 8px; border-radius: 50%; background: {success_color};"></div>
            <span style="font-size: 0.8125rem; font-weight: 600; color: {text_color};">
                All Systems Operational
            </span>
        </div>
        <p style="font-size: 0.6875rem; color: {label_color}; margin: 0; padding-left: 1.125rem;">
            Last updated: {datetime.now().strftime('%H:%M:%S')}
        </p>
    </div>
    """, unsafe_allow_html=True)

# Enhanced Header with Gradient Accent
st.markdown(f"""
<div style="background: {header_bg}; padding: 1.5rem 2rem; border-radius: 12px; margin-bottom: 1.5rem;
            border: 1px solid {border_color}; box-shadow: 0 2px 12px rgba(0, 0, 0, 0.06);
            border-top: 3px solid {accent_color};">
    <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 1rem;">
        <div style="flex: 1; min-width: 250px;">
            <h1 style="margin: 0; font-size: 1.75rem; font-weight: 600; color: {text_color}; letter-spacing: -0.02em;">
                Pipeline Integrity Management
            </h1>
            <p style="margin: 0.5rem 0 0 0; font-size: 0.875rem; color: {label_color};">
                Real-Time SCADA Monitoring & Compliance Platform
            </p>
        </div>
        <div style="display: flex; align-items: center; gap: 1.5rem;">
            <div style="text-align: center;">
                <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                           text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.25rem;">
                    System Time
                </div>
                <div style="font-size: 1rem; font-weight: 600; color: {text_color};">
                    {datetime.now().strftime('%I:%M:%S %p')}
                </div>
                <div style="font-size: 0.75rem; color: {label_color};">
                    {datetime.now().strftime('%B %d, %Y')}
                </div>
            </div>
            <div style="width: 1px; height: 50px; background: {border_color};"></div>
            <div style="text-align: center;">
                <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                           text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.25rem;">
                    Status
                </div>
                <div style="display: inline-flex; align-items: center; padding: 0.5rem 1rem; border-radius: 20px;
                            background: {success_color}20; border: 1px solid {success_color}40;">
                    <span style="font-size: 0.75rem; font-weight: 600; color: {success_color};
                                text-transform: uppercase; letter-spacing: 0.05em;">
                        ● OPERATIONAL
                    </span>
                </div>
            </div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Clean Demo Controls
st.markdown(f"""
<div style="background: {card_bg}; padding: 1rem 1.5rem; border-radius: 12px; margin-bottom: 1.5rem;
            border: 0.5px solid {border_color}; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);">
    <div style="display: flex; align-items: center; gap: 0.75rem;">
        <span style="font-size: 0.875rem; font-weight: 600; color: {text_color};">Demo Controls:</span>
    </div>
</div>
""", unsafe_allow_html=True)

control_col1, control_col2, control_col3 = st.columns([2, 2, 3])

with control_col1:
    demo_label = "Stop Auto-Demo" if st.session_state.demo_mode else "Start Auto-Demo"
    demo_icon = "⏸" if st.session_state.demo_mode else "▶"
    if st.button(f"{demo_icon} {demo_label}", key="demo_toggle", use_container_width=True):
        st.session_state.demo_mode = not st.session_state.demo_mode
        st.session_state.demo_step = 0
        st.rerun()

with control_col2:
    sim_label = "Stop Simulator" if st.session_state.simulator_active else "Start Live Simulator"
    sim_icon = "⏸" if st.session_state.simulator_active else "▶"
    if st.button(f"{sim_icon} {sim_label}", key="sim_toggle", use_container_width=True):
        st.session_state.simulator_active = not st.session_state.simulator_active
        if st.session_state.simulator_active:
            st.rerun()

with control_col3:
    if st.session_state.simulator_active:
        st.session_state.simulator_interval = st.select_slider(
            "Refresh Rate",
            options=[2, 5, 10, 30],
            value=st.session_state.simulator_interval,
            format_func=lambda x: f"{x}s",
            label_visibility="collapsed"
        )

# Auto-Demo Banner - Drift Story Timeline
if st.session_state.demo_mode:
    # Drift Story Timeline from populate_demo_data.py
    drift_story = [
        {
            "time": "10:00 AM",
            "title": "Normal Operations",
            "description": "All segments operating within safe limits (70-75% MAOP)",
            "seg01": 750.0, "seg02": 700.0, "seg03": 650.0, "seg04": 825.0,
            "status": "normal",
            "details": "Baseline readings established. All systems nominal."
        },
        {
            "time": "10:02 AM",
            "title": "⚠️ WARNING - SEG-02",
            "description": "SEG-02 crosses 90% MAOP threshold (855 PSIG)",
            "seg01": 755.0, "seg02": 855.0, "seg03": 652.0, "seg04": 828.0,
            "status": "warning",
            "details": "SEG-02 exceeds 90% MAOP. Monitoring for sustained pressure increase."
        },
        {
            "time": "10:03 AM",
            "title": "📊 TRANSIENT SPIKE - SEG-01 (Filtered)",
            "description": "SEG-01 spikes to 96.5% MAOP but 5-min average remains normal",
            "seg01": 965.0, "seg02": 860.0, "seg03": 653.0, "seg04": 829.0,
            "status": "spike_filtered",
            "details": "Smart Filter: Spike detected but moving average is 76%. No alarm triggered - likely valve operation."
        },
        {
            "time": "10:04 AM",
            "title": "✓ Spike Confirmed Transient",
            "description": "SEG-01 returns to normal - proves spike was transient",
            "seg01": 757.0, "seg02": 870.0, "seg03": 654.0, "seg04": 830.0,
            "status": "normal",
            "details": "SEG-01 back to 75.7% MAOP. Transient filter prevented nuisance alarm. SEG-02 continuing upward trend."
        },
        {
            "time": "10:07 AM",
            "title": "🔴 CRITICAL - SEG-02",
            "description": "SEG-02 crosses 95% MAOP threshold (902.5 PSIG) - SUSTAINED",
            "seg01": 760.0, "seg02": 902.5, "seg03": 658.0, "seg04": 832.0,
            "status": "critical",
            "details": "CRITICAL: SEG-02 at 95% MAOP. 5-min average confirms sustained drift. Compliance clock started."
        },
        {
            "time": "10:09 AM",
            "title": "📊 TRANSIENT SPIKE - SEG-04 (Filtered)",
            "description": "SEG-04 spikes to 97.3% MAOP but 5-min average remains normal",
            "seg01": 761.0, "seg02": 925.0, "seg03": 659.0, "seg04": 1070.0,
            "status": "spike_filtered",
            "details": "Smart Filter: SEG-04 spike detected but moving average is 76%. No alarm - likely pump start. SEG-02 still climbing."
        },
        {
            "time": "10:10 AM",
            "title": "✓ Spike Confirmed Transient",
            "description": "SEG-04 returns to normal - SEG-02 approaching 100% MAOP",
            "seg01": 762.0, "seg02": 940.0, "seg03": 660.0, "seg04": 837.0,
            "status": "critical",
            "details": "SEG-04 back to 76.1% MAOP. Transient filter prevented second nuisance alarm. SEG-02 at 98.9% - critical situation."
        },
        {
            "time": "10:12 AM",
            "title": "❌ VIOLATION - SEG-02",
            "description": "SEG-02 crosses 100% MAOP (955 PSIG) - REGULATORY VIOLATION",
            "seg01": 765.0, "seg02": 955.0, "seg03": 662.0, "seg04": 838.0,
            "status": "violation",
            "details": "VIOLATION: SEG-02 exceeds MAOP (100.5%). Immediate action required. Regulatory reporting triggered."
        }
    ]

    current_story = drift_story[st.session_state.demo_timeline_index % len(drift_story)]

    # Determine status color
    if current_story["status"] == "violation":
        status_color = danger_color
        status_bg = f"{danger_color}20"
    elif current_story["status"] == "critical":
        status_color = "#ff9500"
        status_bg = "#ff950020"
    elif current_story["status"] == "warning":
        status_color = warning_color
        status_bg = f"{warning_color}20"
    elif current_story["status"] == "spike_filtered":
        status_color = accent_color
        status_bg = f"{accent_color}20"
    else:
        status_color = success_color
        status_bg = f"{success_color}20"

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, {status_color} 0%, {status_color}CC 100%);
                padding: 1.5rem 1.75rem; border-radius: 12px; color: white; margin-bottom: 1.5rem;
                box-shadow: 0 4px 16px {status_color}40;">
        <div style="display: flex; align-items: flex-start; justify-content: space-between; gap: 1.5rem;">
            <div style="flex: 1;">
                <div style="font-size: 0.6875rem; font-weight: 700; opacity: 0.95; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
                    DRIFT STORY DEMO • STEP {st.session_state.demo_timeline_index + 1} OF {len(drift_story)}
                </div>
                <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.75rem;">
                    <h3 style="margin: 0; font-size: 1.25rem; font-weight: 700;">
                        {current_story["time"]}
                    </h3>
                    <span style="background: rgba(255,255,255,0.25); padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.75rem; font-weight: 600;">
                        {current_story["title"]}
                    </span>
                </div>
                <p style="margin: 0 0 0.75rem 0; font-size: 0.9375rem; font-weight: 500; opacity: 0.95;">
                    {current_story["description"]}
                </p>
                <div style="background: rgba(0,0,0,0.15); padding: 0.75rem 1rem; border-radius: 8px; font-size: 0.8125rem; line-height: 1.5;">
                    {current_story["details"]}
                </div>
            </div>
            <div style="min-width: 200px; background: rgba(255,255,255,0.15); padding: 1rem; border-radius: 10px;">
                <div style="font-size: 0.6875rem; font-weight: 600; opacity: 0.9; margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;">
                    Pressure Readings
                </div>
                <div style="display: flex; flex-direction: column; gap: 0.5rem; font-size: 0.8125rem;">
                    <div style="display: flex; justify-content: space-between;"><span>SEG-01:</span><strong>{current_story["seg01"]:.1f} PSIG</strong></div>
                    <div style="display: flex; justify-content: space-between;"><span>SEG-02:</span><strong>{current_story["seg02"]:.1f} PSIG</strong></div>
                    <div style="display: flex; justify-content: space-between;"><span>SEG-03:</span><strong>{current_story["seg03"]:.1f} PSIG</strong></div>
                    <div style="display: flex; justify-content: space-between;"><span>SEG-04:</span><strong>{current_story["seg04"]:.1f} PSIG</strong></div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 1])
    with nav_col1:
        if st.button("← Previous Step", use_container_width=True):
            st.session_state.demo_timeline_index = max(0, st.session_state.demo_timeline_index - 1)
            st.rerun()
    with nav_col2:
        if st.button("🔄 Restart Demo", use_container_width=True):
            st.session_state.demo_timeline_index = 0
            st.rerun()
    with nav_col3:
        if st.button("Next Step →", use_container_width=True, type="primary"):
            if st.session_state.demo_timeline_index < len(drift_story) - 1:
                st.session_state.demo_timeline_index += 1
            else:
                st.session_state.demo_timeline_index = 0
            st.rerun()

# Simulator Status
if st.session_state.simulator_active:
    st.markdown(f"""
    <div style="background: {success_color}; padding: 0.75rem 1rem; border-radius: 10px;
                color: white; margin-bottom: 1.5rem; text-align: center; font-weight: 500;
                font-size: 0.8125rem; box-shadow: 0 2px 8px {success_color}40;">
        ● LIVE - Data refreshing every {st.session_state.simulator_interval}s
    </div>
    """, unsafe_allow_html=True)

    import time
    time.sleep(st.session_state.simulator_interval)
    st.rerun()

# Load rules
script_dir = os.path.dirname(os.path.abspath(__file__))
rules_path = os.path.join(script_dir, "rules.json")
thresholds = load_rules(rules_path)

# Quick Stats Banner
df_quick_check = get_all_readings() if 'get_all_readings' in dir() else pd.DataFrame()
if not df_quick_check.empty:
    quick_stats_col1, quick_stats_col2, quick_stats_col3, quick_stats_col4, quick_stats_col5 = st.columns(5)

    with quick_stats_col1:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {accent_color}20 0%, {accent_color}05 100%);
                    padding: 1rem; border-radius: 10px; border-left: 3px solid {accent_color};
                    text-align: center;">
            <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                       text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
                Total Readings
            </div>
            <div style="font-size: 1.5rem; font-weight: 700; color: {accent_color};">
                {len(df_quick_check):,}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with quick_stats_col2:
        segments_count = df_quick_check['SegmentID'].nunique()
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {success_color}20 0%, {success_color}05 100%);
                    padding: 1rem; border-radius: 10px; border-left: 3px solid {success_color};
                    text-align: center;">
            <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                       text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
                Active Segments
            </div>
            <div style="font-size: 1.5rem; font-weight: 700; color: {success_color};">
                {segments_count}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with quick_stats_col3:
        warnings_count = len(df_quick_check[(df_quick_check['Ratio'] >= 90) & (df_quick_check['Ratio'] < 95)])
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {warning_color}20 0%, {warning_color}05 100%);
                    padding: 1rem; border-radius: 10px; border-left: 3px solid {warning_color};
                    text-align: center;">
            <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                       text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
                Warnings
            </div>
            <div style="font-size: 1.5rem; font-weight: 700; color: {warning_color};">
                {warnings_count}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with quick_stats_col4:
        critical_count = len(df_quick_check[(df_quick_check['Ratio'] >= 95) & (df_quick_check['Ratio'] < 100)])
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #ff9500 20%, #ff950005 100%);
                    padding: 1rem; border-radius: 10px; border-left: 3px solid #ff9500;
                    text-align: center;">
            <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                       text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
                Critical
            </div>
            <div style="font-size: 1.5rem; font-weight: 700; color: #ff9500;">
                {critical_count}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with quick_stats_col5:
        violations_count = len(df_quick_check[df_quick_check['Ratio'] >= 100])
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {danger_color}20 0%, {danger_color}05 100%);
                    padding: 1rem; border-radius: 10px; border-left: 3px solid {danger_color};
                    text-align: center;">
            <div style="font-size: 0.6875rem; font-weight: 600; color: {label_color};
                       text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
                Violations
            </div>
            <div style="font-size: 1.5rem; font-weight: 700; color: {danger_color};">
                {violations_count}
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin: 1.5rem 0;'></div>", unsafe_allow_html=True)

# Database query functions
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
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

            # Apply segment filter
            if st.session_state.selected_segments:
                df = df[df['SegmentID'].isin(st.session_state.selected_segments)]

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
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

            # Apply segment filter
            if st.session_state.selected_segments:
                df = df[df['SegmentID'].isin(st.session_state.selected_segments)]

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
            if 'HealthScore' in df.columns:
                df['HealthScore'] = df['HealthScore'].astype(float)
            if 'DaysSinceCalibration' in df.columns:
                df['DaysSinceCalibration'] = df['DaysSinceCalibration'].astype(int)

            # Apply segment filter
            if st.session_state.selected_segments:
                df = df[df['SegmentID'].isin(st.session_state.selected_segments)]

            return df
        return pd.DataFrame()


def get_assets_with_gps():
    """Get all assets with GPS coordinates and current status."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT
                a.AssetID,
                a.SegmentID,
                a.Name as SegmentName,
                a.PipeGrade,
                a.DiameterInches,
                a.WallThicknessInches,
                a.MAOP_PSIG,
                a.ClassLocation,
                a.GPSLatitude,
                a.GPSLongitude,
                (SELECT TOP 1 r.PressurePSIG
                 FROM dbo.Readings r
                 WHERE r.SegmentID = a.SegmentID
                 ORDER BY r.Timestamp DESC) as CurrentPressure,
                (SELECT TOP 1 r.Timestamp
                 FROM dbo.Readings r
                 WHERE r.SegmentID = a.SegmentID
                 ORDER BY r.Timestamp DESC) as LastReadingTime
            FROM dbo.Assets a
            ORDER BY a.SegmentID
        """
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()

        if data:
            df = pd.DataFrame.from_records(data, columns=columns)
            numeric_cols = ['DiameterInches', 'WallThicknessInches', 'MAOP_PSIG', 'GPSLatitude', 'GPSLongitude', 'CurrentPressure']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

            df['CurrentRatio'] = (df['CurrentPressure'] / df['MAOP_PSIG'] * 100).fillna(0)
            df['Status'] = df['CurrentRatio'].apply(
                lambda x: 'VIOLATION' if x >= 100 else 'CRITICAL' if x >= 95 else 'WARNING' if x >= 90 else 'NORMAL'
            )

            # Apply segment filter
            if st.session_state.selected_segments:
                df = df[df['SegmentID'].isin(st.session_state.selected_segments)]

            return df
        return pd.DataFrame()


def get_unacknowledged_alerts():
    """Get all critical alerts that haven't been acknowledged."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
        query = """
            SELECT
                r.ReadingID,
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
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM dbo.AuditTrail at
                        WHERE at.RecordID = r.SegmentID
                        AND at.EventType = 'OPERATOR_ACKNOWLEDGMENT'
                        AND at.Timestamp >= r.Timestamp
                    ) THEN 'Acknowledged'
                    ELSE 'Pending'
                END as AckStatus
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
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

            # Apply segment filter
            if st.session_state.selected_segments:
                df = df[df['SegmentID'].isin(st.session_state.selected_segments)]

            return df
        return pd.DataFrame()


def get_alert_response_metrics():
    """Calculate alert response times and acknowledgment rates."""
    db_conn = get_default_connection()

    with db_conn as conn:
        cursor = conn.cursor()
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
            numeric_cols = ['PressurePSIG', 'MAOP_PSIG', 'Ratio']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

            if 'AckTime' in df.columns:
                df['ResponseTimeMinutes'] = (pd.to_datetime(df['AckTime']) - pd.to_datetime(df['AlertTime'])).dt.total_seconds() / 60

            # Apply segment filter
            if st.session_state.selected_segments:
                df = df[df['SegmentID'].isin(st.session_state.selected_segments)]

            return df
        return pd.DataFrame()


def predict_next_pressure(segment_data):
    """Simple linear regression to predict next pressure reading."""
    if len(segment_data) < 2:
        return None

    segment_data = segment_data.copy()
    segment_data['TimeNumeric'] = (segment_data['Timestamp'] - segment_data['Timestamp'].min()).dt.total_seconds()

    x = segment_data['TimeNumeric'].values
    y = segment_data['PressurePSIG'].astype(float).values

    slope = np.polyfit(x, y, 1)[0]
    intercept = np.polyfit(x, y, 1)[1]

    last_time = x[-1]
    time_delta = (x[-1] - x[-2]) if len(x) > 1 else 300

    future_times = [last_time + time_delta, last_time + (time_delta * 2)]
    future_pressures = [slope * t + intercept for t in future_times]

    future_timestamps = [
        segment_data['Timestamp'].max() + timedelta(seconds=time_delta),
        segment_data['Timestamp'].max() + timedelta(seconds=time_delta * 2)
    ]

    return future_timestamps, future_pressures, slope

# Add horizontal scrollbar for tabs
st.markdown(f"""
<style>
    /* Make tabs scrollable with visible scrollbar */
    .stTabs [data-baseweb="tab-list"] {{
        overflow-x: auto;
        scroll-behavior: smooth;
        scrollbar-width: thin;
        scrollbar-color: {accent_color}40 transparent;
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {{
        height: 6px;
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {{
        background: transparent;
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {{
        background: {accent_color}40;
        border-radius: 3px;
    }}

    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {{
        background: {accent_color}80;
    }}
</style>
""", unsafe_allow_html=True)

# Create tabs - Professional labels without emojis
# Engineering Reconciliation moved to position 2 for immediate visibility
tab_exec, tab_notes, tab_pulse, tab_alerts, tab_map, tab_schematic, tab_alerts_mgmt, tab_ledger, tab_activity, tab_sensors = st.tabs([
    "Executive Summary",
    "Engineering Reconciliation",
    "Live Pulse",
    "Drift Alerts",
    "System Map",
    "Pipeline Schematic",
    "Alert Management",
    "Compliance Ledger",
    "Operator Activity",
    "Sensor Health"
])

# TAB 0: EXECUTIVE SUMMARY
with tab_exec:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Executive Summary</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Real-time compliance overview and financial impact analysis</p>", unsafe_allow_html=True)

    df_readings = get_all_readings()

    if not df_readings.empty:
        # Top-level KPIs
        st.markdown(f"<h3 style='font-size: 1rem; margin-bottom: 1rem;'>Key Performance Indicators</h3>", unsafe_allow_html=True)

        col1, col2, col3, col4, col5 = st.columns(5)

        total_readings = len(df_readings)
        segments = df_readings['SegmentID'].nunique()
        violations = len(df_readings[df_readings['Ratio'] >= 100])
        critical_events = len(df_readings[df_readings['Ratio'] >= 95])

        compliance_score = ((total_readings - critical_events) / total_readings * 100) if total_readings > 0 else 100

        col1.metric("Total Readings", f"{total_readings:,}")
        col2.metric("Pipeline Segments", segments)
        col3.metric("Compliance Score", f"{compliance_score:.1f}%",
                   delta=f"{compliance_score - 85:.1f}%", delta_color="normal")
        col4.metric("Critical Events", critical_events, delta=f"{critical_events}", delta_color="inverse")
        col5.metric("Violations", violations, delta=f"{violations}", delta_color="inverse")

        st.markdown("---")

        # Financial Impact
        st.markdown(f"<h3 style='font-size: 1rem; margin-bottom: 1rem;'>Financial Impact Analysis</h3>", unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3)

        violation_cost = violations * 25000
        critical_cost = critical_events * 5000
        preventive_savings = (total_readings - critical_events) * 100

        col1.metric("Violation Fines", f"${violation_cost:,}",
                   help="Estimated regulatory fines for MAOP violations")
        col2.metric("Inspection Costs", f"${critical_cost:,}",
                   help="Required inspection costs for critical events")
        col3.metric("Preventive Savings", f"${preventive_savings:,}",
                   help="Savings from early warning and prevention")

        net_impact = preventive_savings - violation_cost - critical_cost
        roi_percentage = (net_impact / max(1, violation_cost + critical_cost)) * 100 if (violation_cost + critical_cost) > 0 else 0

        st.metric("Net Financial Impact", f"${net_impact:,}",
                 delta=f"ROI: {roi_percentage:.1f}%",
                 delta_color="normal" if net_impact > 0 else "inverse",
                 help="Total savings minus costs")

        st.markdown("---")

        # Risk Heatmap
        st.markdown(f"<h3 style='font-size: 1rem; margin-bottom: 1rem;'>Risk Heatmap by Segment</h3>", unsafe_allow_html=True)

        segment_risk = df_readings.groupby('SegmentID').agg({
            'Ratio': ['max', 'mean', 'std'],
            'SegmentName': 'first',
            'MAOP_PSIG': 'first'
        }).reset_index()

        segment_risk.columns = ['SegmentID', 'MaxRatio', 'AvgRatio', 'StdRatio', 'Name', 'MAOP']
        segment_risk['MaxRatio'] = segment_risk['MaxRatio'].astype(float)
        segment_risk['AvgRatio'] = segment_risk['AvgRatio'].astype(float)
        segment_risk['StdRatio'] = segment_risk['StdRatio'].astype(float)

        segment_risk['RiskScore'] = (
            segment_risk['MaxRatio'] * 0.5 +
            segment_risk['AvgRatio'] * 0.3 +
            (segment_risk['StdRatio'] * 5) * 0.2
        ).clip(0, 100)

        segment_risk = segment_risk.sort_values('RiskScore', ascending=False)

        fig_heatmap = go.Figure()

        colors = []
        for score in segment_risk['RiskScore']:
            if score >= 95:
                colors.append(danger_color)
            elif score >= 90:
                colors.append(warning_color)
            elif score >= 75:
                colors.append('#ffcc00')
            else:
                colors.append(success_color)

        fig_heatmap.add_trace(go.Bar(
            x=segment_risk['SegmentID'],
            y=segment_risk['RiskScore'],
            marker=dict(color=colors, line=dict(color=border_color, width=1)),
            text=segment_risk['RiskScore'].round(1),
            textposition='outside',
            textfont=dict(size=12, color=text_color, family='Inter'),
            hovertemplate=
                '<b>%{x}</b><br>' +
                'Risk Score: %{y:.1f}<br>' +
                '<extra></extra>'
        ))

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'

        fig_heatmap.update_layout(
            xaxis=dict(title="Pipeline Segment", gridcolor=border_color),
            yaxis=dict(title="Risk Score", gridcolor=border_color, range=[0, 110]),
            height=400,
            showlegend=False,
            template=chart_template,
            plot_bgcolor=plot_bg,
            paper_bgcolor=card_bg,
            margin=dict(l=50, r=50, t=30, b=50),
            font=dict(family='Inter', color=text_color)
        )

        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.warning("No data available. Run setup_demo.py first.")


# TAB 1: LIVE PULSE
with tab_pulse:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Live Pulse</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Real-time pressure monitoring with AI-powered predictive analytics</p>", unsafe_allow_html=True)

    df_readings = get_all_readings()

    if not df_readings.empty:
        fig = go.Figure()
        segments = df_readings['SegmentID'].unique()

        for segment in segments:
            segment_data = df_readings[df_readings['SegmentID'] == segment].copy()
            segment_name = segment_data.iloc[0]['SegmentName']
            maop = float(segment_data.iloc[0]['MAOP_PSIG'])

            segment_colors = {
                'SEG-01': accent_color,
                'SEG-02': '#ff2d55',
                'SEG-03': '#30d158',
                'SEG-04': '#ffd60a'
            }

            fig.add_trace(go.Scatter(
                x=segment_data['Timestamp'],
                y=segment_data['PressurePSIG'],
                mode='lines+markers',
                name=f'{segment} ({segment_name})',
                line=dict(width=2, color=segment_colors.get(segment, accent_color)),
                marker=dict(size=4)
            ))

            prediction = predict_next_pressure(segment_data)
            if prediction:
                future_times, future_pressures, slope = prediction
                fig.add_trace(go.Scatter(
                    x=future_times,
                    y=future_pressures,
                    mode='lines+markers',
                    name=f'{segment} Prediction',
                    line=dict(dash='dot', width=2, color=segment_colors.get(segment, accent_color)),
                    marker=dict(symbol='diamond', size=6),
                    opacity=0.6
                ))

            fig.add_trace(go.Scatter(
                x=segment_data['Timestamp'],
                y=[maop] * len(segment_data),
                mode='lines',
                name=f'{segment} MAOP',
                line=dict(dash='dash', width=1, color=label_color),
                showlegend=False,
                hoverinfo='skip'
            ))

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'

        fig.update_layout(
            xaxis=dict(title="Time", gridcolor=border_color),
            yaxis=dict(title="Pressure (PSIG)", gridcolor=border_color),
            hovermode='x unified',
            height=500,
            template=chart_template,
            plot_bgcolor=plot_bg,
            paper_bgcolor=card_bg,
            margin=dict(l=50, r=50, t=30, b=50),
            font=dict(family='Inter', color=text_color)
        )

        st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3, col4 = st.columns(4)
        total_readings = len(df_readings)
        violation_count = len(df_readings[df_readings['Ratio'] >= 100])
        critical_count = len(df_readings[(df_readings['Ratio'] >= 95) & (df_readings['Ratio'] < 100)])
        warning_count = len(df_readings[(df_readings['Ratio'] >= 90) & (df_readings['Ratio'] < 95)])

        col1.metric("Total Readings", total_readings)
        col2.metric("Warnings", warning_count)
        col3.metric("Critical", critical_count, delta_color="inverse")
        col4.metric("Violations", violation_count, delta_color="inverse")
    else:
        st.warning("No readings found. Run setup_demo.py first.")


# TAB 2: DRIFT ALERTS
with tab_alerts:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Drift Alerts</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Critical threshold crossings with smart filtering</p>", unsafe_allow_html=True)

    df_alerts = get_drift_alerts()

    if not df_alerts.empty:
        col1, col2, col3 = st.columns(3)
        violation_alerts = len(df_alerts[df_alerts['Status'] == 'VIOLATION'])
        critical_alerts = len(df_alerts[df_alerts['Status'] == 'CRITICAL'])
        affected_segments = df_alerts['SegmentID'].nunique()

        col1.metric("Violation Events", violation_alerts)
        col2.metric("Critical Events", critical_alerts)
        col3.metric("Affected Segments", affected_segments)

        st.markdown("---")

        st.markdown(f"<h3 style='font-size: 1rem; margin-bottom: 1rem;'>Smart Filtering Analysis</h3>", unsafe_allow_html=True)

        df_all_readings = get_all_readings()
        if not df_all_readings.empty:
            filter_summary = get_spike_vs_sustained_summary(df_all_readings, window_minutes=5)
            filtered_data = filter_summary['filtered_data']

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total High Readings", filter_summary['total_high_readings'])
            col2.metric("Spikes Filtered", filter_summary['spikes_filtered'])
            col3.metric("Sustained Flagged", filter_summary['sustained_flagged'])
            col4.metric("Filter Effectiveness", f"{filter_summary['filter_effectiveness']:.0f}%")

        df_display = df_alerts.copy()
        df_display['Timestamp'] = pd.to_datetime(df_display['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_display['Ratio'] = df_display['Ratio'].round(1).astype(str) + '%'
        df_display['PressurePSIG'] = df_display['PressurePSIG'].round(1)

        st.dataframe(
            df_display[['Timestamp', 'SegmentID', 'SegmentName', 'PressurePSIG', 'Ratio', 'Status']],
            use_container_width=True,
            height=400
        )
    else:
        st.success("No critical threshold crossings detected.")


# TAB 3: SYSTEM MAP
with tab_map:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>System Map</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>GPS visualization with real-time status indicators</p>", unsafe_allow_html=True)

    df_assets = get_assets_with_gps()

    if not df_assets.empty:
        col1, col2, col3, col4 = st.columns(4)
        total_segments = len(df_assets)
        segments_normal = len(df_assets[df_assets['Status'] == 'NORMAL'])
        segments_warning = len(df_assets[df_assets['Status'] == 'WARNING'])
        segments_critical = len(df_assets[df_assets['Status'] == 'CRITICAL'])

        col1.metric("Total Segments", total_segments)
        col2.metric("Normal", segments_normal)
        col3.metric("Warning/Critical", segments_warning + segments_critical)
        col4.metric("Violations", len(df_assets[df_assets['Status'] == 'VIOLATION']))

        st.markdown("---")

        fig_map = go.Figure()

        status_colors = {
            'NORMAL': success_color,
            'WARNING': warning_color,
            'CRITICAL': '#ff9500',
            'VIOLATION': danger_color
        }

        for status in ['NORMAL', 'WARNING', 'CRITICAL', 'VIOLATION']:
            status_df = df_assets[df_assets['Status'] == status]
            if not status_df.empty:
                fig_map.add_trace(go.Scattergeo(
                    lon=status_df['GPSLongitude'],
                    lat=status_df['GPSLatitude'],
                    text=status_df['SegmentName'],
                    mode='markers+text',
                    name=status,
                    marker=dict(size=15, color=status_colors[status], line=dict(width=2, color='white')),
                    textposition="top center"
                ))

        center_lat = df_assets['GPSLatitude'].mean()
        center_lon = df_assets['GPSLongitude'].mean()

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'

        fig_map.update_layout(
            geo=dict(
                scope='usa',
                center=dict(lat=center_lat, lon=center_lon),
                projection_scale=15,
                showland=True,
                landcolor='rgb(243, 243, 243)' if not st.session_state.dark_mode else 'rgb(30, 30, 30)'
            ),
            height=500,
            template=chart_template,
            paper_bgcolor=card_bg
        )

        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("No asset data available.")


# TAB 4: PIPELINE SCHEMATIC
with tab_schematic:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Pipeline Schematic</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Network topology and flow characteristics</p>", unsafe_allow_html=True)

    df_assets = get_assets_with_gps()

    if not df_assets.empty:
        positions = {
            'SEG-01': (1, 2),
            'SEG-02': (2, 3),
            'SEG-03': (3, 1),
            'SEG-04': (2, 1)
        }

        connections = [
            ('SEG-01', 'SEG-02'),
            ('SEG-02', 'SEG-03'),
            ('SEG-02', 'SEG-04')
        ]

        fig_schematic = go.Figure()

        for seg1, seg2 in connections:
            if seg1 in positions and seg2 in positions:
                x0, y0 = positions[seg1]
                x1, y1 = positions[seg2]
                fig_schematic.add_trace(go.Scatter(
                    x=[x0, x1], y=[y0, y1],
                    mode='lines',
                    line=dict(width=3, color=label_color),
                    showlegend=False,
                    hoverinfo='skip'
                ))

        for _, segment in df_assets.iterrows():
            seg_id = segment['SegmentID']
            if seg_id in positions:
                x, y = positions[seg_id]
                if segment['Status'] == 'VIOLATION':
                    color = danger_color
                elif segment['Status'] == 'CRITICAL':
                    color = warning_color
                elif segment['Status'] == 'WARNING':
                    color = '#ffcc00'
                else:
                    color = success_color

                fig_schematic.add_trace(go.Scatter(
                    x=[x], y=[y],
                    mode='markers+text',
                    name=seg_id,
                    marker=dict(size=40, color=color, line=dict(width=2, color='white')),
                    text=seg_id,
                    textposition='middle center',
                    textfont=dict(size=10, color='white', family='Inter')
                ))

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'

        fig_schematic.update_layout(
            xaxis=dict(showgrid=False, showticklabels=False, zeroline=False, range=[0.5, 3.5]),
            yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, range=[0.5, 3.5]),
            height=400,
            template=chart_template,
            plot_bgcolor=plot_bg,
            paper_bgcolor=card_bg,
            showlegend=False
        )

        st.plotly_chart(fig_schematic, use_container_width=True)

        st.markdown("---")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Avg Pressure", f"{df_assets['CurrentPressure'].mean():.1f} PSIG")
        col2.metric("Max Pressure", f"{df_assets['CurrentPressure'].max():.1f} PSIG")
        col3.metric("Min Pressure", f"{df_assets['CurrentPressure'].min():.1f} PSIG")
        col4.metric("Variance", f"{df_assets['CurrentPressure'].std():.1f} PSIG")
    else:
        st.warning("No asset data available.")


# TAB 5: ALERT MANAGEMENT
with tab_alerts_mgmt:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Alert Management</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Inbox-style workflow for compliance tracking</p>", unsafe_allow_html=True)

    df_all_alerts = get_unacknowledged_alerts()

    if not df_all_alerts.empty:
        col1, col2, col3, col4 = st.columns(4)
        total_alerts = len(df_all_alerts)
        pending_alerts = len(df_all_alerts[df_all_alerts['AckStatus'] == 'Pending'])
        acknowledged_alerts = len(df_all_alerts[df_all_alerts['AckStatus'] == 'Acknowledged'])
        violation_alerts = len(df_all_alerts[df_all_alerts['AlertLevel'] == 'VIOLATION'])

        col1.metric("Total Alerts", total_alerts)
        col2.metric("Pending", pending_alerts)
        col3.metric("Acknowledged", acknowledged_alerts)
        col4.metric("Violations", violation_alerts)

        st.markdown("---")

        inbox_tab1, inbox_tab2 = st.tabs(["Pending", "All Alerts"])

        with inbox_tab1:
            pending_df = df_all_alerts[df_all_alerts['AckStatus'] == 'Pending']
            if not pending_df.empty:
                for idx, alert in pending_df.iterrows():
                    if alert['AlertLevel'] == 'VIOLATION':
                        alert_color = danger_color
                    elif alert['AlertLevel'] == 'CRITICAL':
                        alert_color = warning_color
                    else:
                        alert_color = '#ffcc00'

                    st.markdown(f"""
                    <div style="background: {card_bg}; border-left: 3px solid {alert_color};
                                padding: 1rem; border-radius: 10px; margin-bottom: 0.75rem;
                                border: 0.5px solid {border_color};">
                        <strong style="color: {text_color};">{alert['AlertLevel']} - {alert['SegmentName']}</strong><br>
                        <span style="color: {label_color}; font-size: 0.875rem;">
                            {pd.to_datetime(alert['AlertTime']).strftime('%Y-%m-%d %H:%M:%S')} |
                            {alert['PressurePSIG']:.1f} PSIG ({alert['Ratio']:.1f}% MAOP)
                        </span>
                    </div>
                    """, unsafe_allow_html=True)

                    if st.button(f"Acknowledge #{alert['ReadingID']}", key=f"ack_{alert['ReadingID']}"):
                        db_conn = get_default_connection()
                        with db_conn as conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                INSERT INTO dbo.AuditTrail
                                (Timestamp, UserID, EventType, TableAffected, RecordID, Details, ChangeReason)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                datetime.now(), 1, 'OPERATOR_ACKNOWLEDGMENT', 'Readings',
                                alert['SegmentID'],
                                f"Acknowledged {alert['AlertLevel']} alarm on {alert['SegmentID']}",
                                "Compliance acknowledgment"
                            ))
                            conn.commit()
                        st.success("Alert acknowledged!")
                        st.rerun()
            else:
                st.success("No pending alerts!")

        with inbox_tab2:
            df_display = df_all_alerts.copy()
            df_display['AlertTime'] = pd.to_datetime(df_display['AlertTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_display['Ratio'] = df_display['Ratio'].round(1).astype(str) + '%'

            st.dataframe(
                df_display[['AlertTime', 'SegmentID', 'AlertLevel', 'Ratio', 'AckStatus']],
                use_container_width=True,
                height=400
            )
    else:
        st.success("No alerts in the system.")


# TAB 6: COMPLIANCE LEDGER
with tab_ledger:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Compliance Ledger</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Cryptographic hash chain verification</p>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("Verify Integrity", type="primary", use_container_width=True):
            with st.spinner("Verifying..."):
                is_valid, broken_at, total_checked = verify_hash_chain()
                if is_valid:
                    st.success(f"✓ Hash chain intact ({total_checked} readings)")
                else:
                    st.error(f"✗ Tampering detected at record {broken_at}")

    with col2:
        st.info("Each reading contains a SHA-256 hash including the previous reading's hash, creating an immutable chain.")

    st.markdown("---")

    df_readings = get_all_readings()
    if not df_readings.empty:
        df_ledger = df_readings.copy()
        df_ledger['Timestamp'] = pd.to_datetime(df_ledger['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_ledger['Hash Preview'] = df_ledger['hash_signature'].apply(lambda x: x[:16] + '...' if x else 'N/A')

        st.dataframe(
            df_ledger[['ReadingID', 'Timestamp', 'SegmentID', 'PressurePSIG', 'Hash Preview']],
            use_container_width=True,
            height=400
        )


# TAB 7: OPERATOR ACTIVITY
with tab_activity:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Operator Activity</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Audit trail and response time monitoring</p>", unsafe_allow_html=True)

    df_activity = get_operator_activity()

    if not df_activity.empty:
        col1, col2, col3, col4 = st.columns(4)
        total_actions = len(df_activity)
        unique_operators = df_activity['Operator'].nunique()
        latest_action = pd.to_datetime(df_activity['Timestamp']).max()
        hours_ago = (datetime.now() - latest_action).total_seconds() / 3600

        col1.metric("Total Actions", total_actions)
        col2.metric("Active Operators", unique_operators)
        col3.metric("Acknowledgments", len(df_activity[df_activity['ActionType'] == 'OPERATOR_ACKNOWLEDGMENT']))
        col4.metric("Last Action", f"{hours_ago:.1f}h ago")

        st.markdown("---")

        df_display = df_activity.copy()
        df_display['Timestamp'] = pd.to_datetime(df_display['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

        st.dataframe(
            df_display[['Timestamp', 'Operator', 'ActionType', 'Description']],
            use_container_width=True,
            height=400
        )
    else:
        st.warning("No operator activity found.")


# TAB 8: SENSOR HEALTH
with tab_sensors:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Sensor Health</h2>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {label_color}; margin-bottom: 1.5rem;'>Calibration status and predictive maintenance</p>", unsafe_allow_html=True)

    df_sensors = get_sensor_health()

    if not df_sensors.empty:
        col1, col2, col3, col4 = st.columns(4)
        avg_health = df_sensors['HealthScore'].mean()
        sensors_current = len(df_sensors[df_sensors['CalibrationStatus'] == 'Current'])
        sensors_due = len(df_sensors[df_sensors['CalibrationStatus'] == 'Due Soon'])
        sensors_overdue = len(df_sensors[df_sensors['CalibrationStatus'] == 'Overdue'])

        col1.metric("System Health", f"{avg_health:.0f}/100")
        col2.metric("Current", f"{sensors_current}/{len(df_sensors)}")
        col3.metric("Due Soon", sensors_due)
        col4.metric("Overdue", sensors_overdue)

        st.markdown("---")

        fig_health = go.Figure()
        colors = [success_color if score >= 95 else warning_color if score >= 85 else danger_color
                  for score in df_sensors['HealthScore']]

        fig_health.add_trace(go.Bar(
            x=df_sensors['SegmentName'],
            y=df_sensors['HealthScore'],
            marker_color=colors,
            text=df_sensors['HealthScore'],
            textposition='outside'
        ))

        chart_template = 'plotly_dark' if st.session_state.dark_mode else 'plotly_white'

        fig_health.update_layout(
            xaxis=dict(title="Segment"),
            yaxis=dict(title="Health Score", range=[0, 105]),
            height=350,
            showlegend=False,
            template=chart_template,
            plot_bgcolor=plot_bg,
            paper_bgcolor=card_bg
        )

        st.plotly_chart(fig_health, use_container_width=True)

        st.markdown("---")

        df_display = df_sensors.copy()
        df_display['LastCalibrationDate'] = pd.to_datetime(df_display['LastCalibrationDate']).dt.strftime('%Y-%m-%d')

        st.dataframe(
            df_display[['SerialNumber', 'SegmentName', 'HealthScore', 'CalibrationStatus', 'LastCalibrationDate']],
            use_container_width=True,
            height=300
        )
    else:
        st.warning("No sensor data found.")


# TAB 9: ENGINEERING RECONCILIATION (Immutable Ledger)
with tab_notes:
    st.markdown(f"<h2 style='margin-bottom: 0.5rem;'>Engineering Reconciliation</h2>", unsafe_allow_html=True)
    st.markdown(f"""
    <p style='color: {label_color}; margin-bottom: 1.5rem;'>
        Immutable engineering notes with version control. Cannot edit - only supersede with corrections.
        <span style='background: {accent_color}20; padding: 0.25rem 0.5rem; border-radius: 4px; font-weight: 600; color: {accent_color}; margin-left: 0.5rem;'>
            APPEND-ONLY LEDGER
        </span>
    </p>
    """, unsafe_allow_html=True)

    # Function to get engineering notes
    def get_engineering_notes():
        """Get engineering reconciliation notes from database."""
        db_conn = get_default_connection()

        with db_conn as conn:
            cursor = conn.cursor()
            query = """
                SELECT
                    e.NoteID,
                    e.Timestamp,
                    e.ReconcilerID,
                    e.ReconcilerName,
                    e.AssetID,
                    a.Name as AssetName,
                    e.QI_Status,
                    e.NoteText,
                    e.VersionNumber,
                    e.SupersededByID,
                    e.Status,
                    e.ReadingID,
                    e.OriginalDataHash,
                    e.ReconciliationHash
                FROM dbo.EngineeringReconciliation e
                JOIN dbo.Assets a ON e.AssetID = a.SegmentID
                ORDER BY e.Timestamp DESC
            """
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return pd.DataFrame(data) if data else pd.DataFrame()

    # Function to add engineering note with hash sealing
    def add_engineering_note(reconciler_id, reconciler_name, asset_id, qi_status, note_text, reading_id=None, supersedes_id=None):
        """Add a new engineering reconciliation note with cryptographic hash sealing (IMMUTABLE - insert only)."""
        import hashlib
        from datetime import datetime

        db_conn = get_default_connection()

        with db_conn as conn:
            cursor = conn.cursor()

            # If this is superseding an old note, get the next version number
            version_number = 1
            if supersedes_id:
                cursor.execute("""
                    SELECT VersionNumber FROM dbo.EngineeringReconciliation
                    WHERE NoteID = ?
                """, (supersedes_id,))
                result = cursor.fetchone()
                if result:
                    version_number = result[0] + 1

            # Get OriginalDataHash from the Reading if ReadingID is provided
            original_data_hash = None
            if reading_id:
                cursor.execute("""
                    SELECT hash_signature FROM dbo.Readings WHERE ReadingID = ?
                """, (reading_id,))
                result = cursor.fetchone()
                if result:
                    original_data_hash = result[0]

            # Get current timestamp
            timestamp = datetime.now()

            # Calculate ReconciliationHash (SHA-256 seal on this note)
            data_string = f"{note_text}|{reading_id if reading_id else 0}|{timestamp.isoformat()}|{reconciler_id}"
            reconciliation_hash = hashlib.sha256(data_string.encode('utf-8')).hexdigest()

            # Insert new note with hash sealing
            cursor.execute("""
                INSERT INTO dbo.EngineeringReconciliation
                (ReconcilerID, ReconcilerName, AssetID, QI_Status, NoteText, VersionNumber,
                 Status, ReadingID, OriginalDataHash, ReconciliationHash)
                OUTPUT INSERTED.NoteID
                VALUES (?, ?, ?, ?, ?, ?, 'CURRENT', ?, ?, ?)
            """, (reconciler_id, reconciler_name, asset_id, qi_status, note_text, version_number,
                  reading_id, original_data_hash, reconciliation_hash))

            new_note_id = cursor.fetchone()[0]

            # If superseding, update the old note's SupersededByID and Status
            # The trigger allows ONLY these two fields to be updated
            if supersedes_id:
                cursor.execute("""
                    UPDATE dbo.EngineeringReconciliation
                    SET SupersededByID = ?, Status = 'SUPERSEDED'
                    WHERE NoteID = ?
                """, (new_note_id, supersedes_id))

            conn.commit()
            return new_note_id

    # Explanation banner
    with st.expander("💡 How Immutable Ledger Works", expanded=False):
        st.markdown(f"""
        <div style="background: {secondary_bg}; padding: 1rem; border-radius: 8px; border-left: 4px solid {accent_color};">
            <h4 style="margin-top: 0; color: {accent_color};">Forensic-Grade Engineering Documentation</h4>

            <p><strong>Immutability:</strong> You cannot edit or delete notes. If you make a mistake, click "Supersede" to add a correction that references the original.</p>

            <p><strong>Cryptographic Sealing:</strong> Each note is sealed with two SHA-256 hashes:</p>
            <ul>
                <li><strong>ReconciliationHash</strong> - Unique seal calculated from: NoteText + ReadingID + Timestamp + ReconcilerID</li>
                <li><strong>OriginalDataHash</strong> - Links to the raw sensor reading's hash_signature (if linked to a Reading)</li>
            </ul>

            <p><strong>Version Control:</strong> Corrections create new versions that supersede old ones. The system tracks the complete chain of thought, proving you didn't hide mistakes.</p>

            <p><strong>Qualified Inspector Workflow:</strong></p>
            <ol style="margin-bottom: 0;">
                <li><strong>Pending</strong> → Engineer creates note</li>
                <li><strong>QI_Reviewing</strong> → QI actively examining</li>
                <li><strong>QI_Approved</strong> → Certified for compliance</li>
                <li><strong>QI_Rejected</strong> → Needs revision (create new version)</li>
                <li><strong>Closed</strong> → Final state, archived</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

    # Add new note form
    st.markdown(f"### Add Engineering Note")

    with st.form("add_note_form", clear_on_submit=True):
        form_col1, form_col2 = st.columns(2)

        with form_col1:
            # Get users for engineer selection
            db_conn = get_default_connection()
            with db_conn as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT UserID, FirstName + ' ' + LastName as Name FROM dbo.Users")
                users = cursor.fetchall()
            user_options = {row.Name: row.UserID for row in users}
            selected_user = st.selectbox("Engineer/Reconciler *", options=list(user_options.keys()))

            note_asset = st.selectbox(
                "Asset/Segment *",
                options=['SEG-01', 'SEG-02', 'SEG-03', 'SEG-04'],
                help="Select the pipeline segment this note applies to"
            )

            note_qi_status = st.selectbox(
                "QI Status *",
                options=['Pending', 'QI_Reviewing', 'QI_Approved', 'QI_Rejected', 'Closed'],
                help="Qualified Inspector review status"
            )

        with form_col2:
            # Optional: Link to a specific Reading
            db_conn = get_default_connection()
            with db_conn as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TOP 50 ReadingID, SegmentID, Timestamp, PressurePSIG, MAOP_PSIG
                    FROM dbo.Readings
                    ORDER BY Timestamp DESC
                """)
                readings = cursor.fetchall()

            reading_options = {"None (General Note)": None}
            for r in readings:
                timestamp_str = r.Timestamp.strftime('%Y-%m-%d %H:%M')
                ratio = (r.PressurePSIG / r.MAOP_PSIG * 100)
                reading_options[f"{r.SegmentID} @ {timestamp_str} ({ratio:.1f}%)"] = r.ReadingID

            selected_reading = st.selectbox(
                "Link to Reading (Optional)",
                options=list(reading_options.keys()),
                help="Link this note to a specific pressure reading (creates forensic chain with OriginalDataHash)"
            )
            reading_id = reading_options[selected_reading]

        note_text = st.text_area(
            "Engineering Note *",
            placeholder="Example: 'Sensor A-101 reading anomaly due to power surge from compressor start-up. Referencing Work Order #554.'",
            height=120,
            help="Be specific and cite work orders, sensor IDs, or other references. This record is immutable and cryptographically sealed."
        )

        submit_button = st.form_submit_button("🔒 Submit Note (Immutable)", use_container_width=True, type="primary")

        if submit_button:
            if not note_text:
                st.error("Please fill in the Engineering Note")
            else:
                try:
                    add_engineering_note(
                        reconciler_id=user_options[selected_user],
                        reconciler_name=selected_user,
                        asset_id=note_asset,
                        qi_status=note_qi_status,
                        note_text=note_text,
                        reading_id=reading_id
                    )
                    st.success(f"✓ Engineering note added to immutable ledger with cryptographic seal!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error adding note: {e}")

    st.markdown("---")

    # Display existing notes
    st.markdown(f"### Engineering Notes Ledger")

    # Filter options
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        show_superseded = st.checkbox("Show Superseded Notes", value=False)
    with filter_col2:
        filter_asset = st.selectbox(
            "Filter by Asset",
            options=["All"] + ['SEG-01', 'SEG-02', 'SEG-03', 'SEG-04'],
            key="note_filter_asset"
        )
    with filter_col3:
        filter_qi_status = st.selectbox(
            "Filter by QI Status",
            options=["All", 'Pending', 'QI_Reviewing', 'QI_Approved', 'QI_Rejected', 'Closed'],
            key="note_filter_qi"
        )

    # Get and filter notes
    try:
        df_notes = get_engineering_notes()

        if not df_notes.empty:
            # Apply filters
            if not show_superseded:
                df_notes = df_notes[df_notes['Status'] == 'CURRENT']

            if filter_asset != "All":
                df_notes = df_notes[df_notes['AssetID'] == filter_asset]

            if filter_qi_status != "All":
                df_notes = df_notes[df_notes['QI_Status'] == filter_qi_status]

            if df_notes.empty:
                st.info("No notes match the current filters")
            else:
                # Group by AssetID to show version chains
                for asset_id in df_notes['AssetID'].unique():
                    asset_notes = df_notes[df_notes['AssetID'] == asset_id].sort_values('VersionNumber', ascending=False)

                    st.markdown(f"#### Asset: {asset_notes.iloc[0]['AssetName']} ({asset_id})")

                    # Display notes as cards with version history
                    for idx, note in asset_notes.iterrows():
                        # QI Status color
                        if note['QI_Status'] == 'QI_Approved':
                            qi_color = success_color
                        elif note['QI_Status'] == 'QI_Rejected':
                            qi_color = danger_color
                        elif note['QI_Status'] == 'QI_Reviewing':
                            qi_color = warning_color
                        else:
                            qi_color = label_color

                        # Status badge
                        if note['Status'] == 'SUPERSEDED':
                            status_badge = f"""
                            <div style="display: inline-block; background: {label_color}20; border: 1px solid {label_color}40;
                                        padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.75rem; color: {label_color}; font-weight: 600;">
                                ⚠️ SUPERSEDED
                            </div>
                            """
                            border_color_note = label_color
                        else:
                            status_badge = f"""
                            <div style="display: inline-block; background: {success_color}20; border: 1px solid {success_color}40;
                                        padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.75rem; color: {success_color}; font-weight: 600;">
                                ✓ CURRENT
                            </div>
                            """
                            border_color_note = accent_color

                        st.markdown(f"""
                        <div style="background: {card_bg}; border-left: 4px solid {border_color_note};
                                    padding: 1.25rem; border-radius: 10px; margin-bottom: 1rem;
                                    border: 1px solid {border_color}; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);">
                            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
                                <div style="flex: 1;">
                                    <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
                                        <span style="background: {qi_color}20; border: 1px solid {qi_color}40;
                                                    padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.75rem;
                                                    color: {qi_color}; font-weight: 600;">
                                            QI: {note['QI_Status'].replace('_', ' ')}
                                        </span>
                                        <span style="background: {accent_color}20; border: 1px solid {accent_color}40;
                                                    padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.75rem;
                                                    color: {accent_color}; font-weight: 600;">
                                            VERSION {note['VersionNumber']}
                                        </span>
                                        {status_badge}
                                        <span style="background: {secondary_bg}; padding: 0.25rem 0.75rem; border-radius: 20px;
                                                    font-size: 0.65rem; color: {label_color}; font-weight: 500;">
                                            ID: {note['NoteID']}
                                        </span>
                                    </div>
                                    <p style="margin: 0.75rem 0; color: {text_color}; line-height: 1.6; font-size: 0.9375rem;
                                              background: {secondary_bg}40; padding: 0.75rem; border-radius: 6px;">
                                        {note['NoteText']}
                                    </p>
                                </div>
                            </div>
                            <div style="padding-top: 0.75rem; border-top: 1px solid {border_color};
                                        font-size: 0.8125rem; color: {label_color};">
                                <div style="margin-bottom: 0.5rem;">
                                    <strong>Timestamp:</strong> {pd.to_datetime(note['Timestamp']).strftime('%Y-%m-%d %H:%M:%S')} |
                                    <strong>Engineer:</strong> {note['ReconcilerName']} |
                                    <strong>Asset:</strong> {note['AssetName']}
                                    {f"| <strong>Reading ID:</strong> {note['ReadingID']}" if note.get('ReadingID') else ""}
                                </div>
                                <div style="font-family: 'Courier New', monospace; font-size: 0.7rem; background: {secondary_bg};
                                            padding: 0.5rem; border-radius: 4px; margin-top: 0.5rem;">
                                    <div style="margin-bottom: 0.25rem;">
                                        <strong>🔒 ReconciliationHash:</strong> {note.get('ReconciliationHash', 'N/A')[:32]}...
                                    </div>
                                    {f"<div><strong>🔗 OriginalDataHash:</strong> {note.get('OriginalDataHash', 'N/A')[:32]}...</div>" if note.get('OriginalDataHash') else ""}
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                        # Supersede button (only for CURRENT notes)
                        if note['Status'] == 'CURRENT':
                            if st.button(f"📝 Supersede with Correction", key=f"supersede_{note['NoteID']}", use_container_width=True):
                                st.session_state[f'superseding_{note['NoteID']}'] = True
                                st.rerun()

                            # Show supersede form if button was clicked
                            if st.session_state.get(f'superseding_{note['NoteID']}', False):
                                with st.form(f"supersede_form_{note['NoteID']}", clear_on_submit=True):
                                    st.markdown(f"**Superseding Note ID {note['NoteID']} (Version {note['VersionNumber']})**")
                                    correction_text = st.text_area(
                                        "Correction Note",
                                        placeholder=f"Example: 'Correction to Note #{note['NoteID']}: The issue was not a power surge, but a faulty sensor. Replaced sensor A-101. Work Order #555.'",
                                        height=100
                                    )
                                    super_col1, super_col2 = st.columns(2)
                                    with super_col1:
                                        if st.form_submit_button("Submit Correction", type="primary", use_container_width=True):
                                            if correction_text:
                                                try:
                                                    add_engineering_note(
                                                        reconciler_id=note['ReconcilerID'],
                                                        reconciler_name=note['ReconcilerName'],
                                                        asset_id=note['AssetID'],
                                                        qi_status=note['QI_Status'],
                                                        note_text=correction_text,
                                                        reading_id=note.get('ReadingID'),
                                                        supersedes_id=note['NoteID']
                                                    )
                                                    st.session_state[f'superseding_{note['NoteID']}'] = False
                                                    st.success(f"✓ Correction added as Version {note['VersionNumber'] + 1} with cryptographic seal")
                                                    st.rerun()
                                                except Exception as e:
                                                    st.error(f"Error: {e}")
                                    with super_col2:
                                        if st.form_submit_button("Cancel", use_container_width=True):
                                            st.session_state[f'superseding_{note['NoteID']}'] = False
                                            st.rerun()

                    st.markdown("---")
        else:
            st.info("No engineering notes yet. Add your first note above!")

    except Exception as e:
        st.warning(f"⚠️ EngineeringReconciliation table not found. Please run: create_sticky_notes_table.sql")
        st.code("""
# Run this SQL script to create the immutable EngineeringReconciliation table:
cd demo_functionality
sqlcmd -S localhost\\SQLEXPRESS -i create_sticky_notes_table.sql

# Or execute the script manually in SQL Server Management Studio
        """, language="bash")

    # Add explanation about the 5-second simulator
    with st.expander("ℹ️ What does the Real-Time Simulator do?"):
        st.markdown(f"""
        **Real-Time Data Simulator** (5-second refresh):

        When you click **"Start Real-Time Simulator"** in the demo controls above, the dashboard will:

        - Auto-refresh every 5 seconds to simulate live SCADA data updates
        - Show changing timestamps in the "LIVE" indicator
        - Demonstrate how operators monitor pipelines in real-time
        - Perfect for demos and presentations to show dynamic data flow

        **Note:** This doesn't change the actual database data - it just refreshes the view to simulate a live monitoring system.
        The actual drift story data remains in the database as you populated it.

        Turn it **OFF** when you want to pause and examine data in detail.
        """)


# Footer
st.markdown(f"""
<div style="text-align: center; padding: 2rem; color: {label_color}; font-size: 0.75rem; margin-top: 3rem; border-top: 0.5px solid {border_color};">
    <strong>ClearLine Pipeline Management System</strong><br>
    Enterprise compliance monitoring platform<br>
    © 2026 ClearLine Technologies
</div>
""", unsafe_allow_html=True)
