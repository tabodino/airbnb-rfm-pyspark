# Color Palette - Neutral & Professional
COLORS = {
    "primary": "#484848",  # Airbnb dark gray
    "secondary": "#767676",  # Medium gray
    "accent": "#FF5A5F",  # Airbnb coral (subtle use)
    "success": "#00A699",  # Teal
    "warning": "#FC642D",  # Orange
    "danger": "#C13515",  # Red
    "background": "#FFFFFF",
    "surface": "#F7F7F7",
    "border": "#EBEBEB",
    "text_primary": "#222222",
    "text_secondary": "#717171",
}

# Typography
FONTS = {
    "primary": "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    "mono": "'SF Mono', 'Consolas', 'Monaco', monospace",
}

# Segment Colors - Muted, professional palette
SEGMENT_COLORS = {
    "Champions": "#2E7D32",
    "Loyal": "#1976D2",
    "Potential": "#0288D1",
    "Promising": "#00897B",
    "Need Attention": "#F57C00",
    "At Risk": "#E64A19",
    "Hibernating": "#757575",
    "Lost": "#424242",
}


def get_custom_css() -> str:
    """Returns professional CSS styling for the dashboard"""
    return f"""
    <style>
    /* Global Resets & Base Styles */
    .main {{
        background-color: {COLORS['background']};
        font-family: {FONTS['primary']};
    }}
    
    /* Remove Streamlit Branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {{
        color: {COLORS['text_primary']};
        font-weight: 600;
        letter-spacing: -0.02em;
    }}
    
    h1 {{
        font-size: 2rem;
        margin-bottom: 0.5rem;
    }}
    
    h2 {{
        font-size: 1.5rem;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }}
    
    h3 {{
        font-size: 1.25rem;
        margin-bottom: 0.75rem;
    }}
    
    p, .stMarkdown {{
        color: {COLORS['text_secondary']};
        font-size: 0.95rem;
        line-height: 1.6;
    }}
    
    /* Card Styling */
    .metric-card {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1.25rem;
        transition: box-shadow 0.2s ease;
    }}
    
    .metric-card:hover {{
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }}
    
    /* Metrics */
    [data-testid="stMetric"] {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 6px;
        padding: 1rem;
    }}
    
    [data-testid="stMetricLabel"] {{
        color: {COLORS['text_secondary']};
        font-size: 0.85rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}
    
    [data-testid="stMetricValue"] {{
        color: {COLORS['text_primary']};
        font-size: 1.75rem;
        font-weight: 600;
    }}
    
    /* Tables */
    .stDataFrame {{
        border: 1px solid {COLORS['border']};
        border-radius: 6px;
        overflow: hidden;
    }}
    
    /* Buttons */
    .stButton > button {{
        background-color: {COLORS['primary']};
        color: white;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        transition: background-color 0.2s ease;
    }}
    
    .stButton > button:hover {{
        background-color: {COLORS['accent']};
    }}
    
    /* Dividers */
    hr {{
        margin: 2rem 0;
        border: none;
        border-top: 1px solid {COLORS['border']};
    }}
    
    /* Sidebar Minimal */
    [data-testid="stSidebar"] {{
        background-color: {COLORS['surface']};
        border-right: 1px solid {COLORS['border']};
    }}
    
    /* Remove excessive padding */
    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }}
    
    /* Clean selectbox */
    .stSelectbox {{
        border-radius: 6px;
    }}
    
    /* Plotly charts - remove unnecessary UI */
    .modebar {{
        display: none !important;
    }}
    </style>
    """


def get_chart_config() -> dict:
    """Standard configuration for all Plotly charts"""
    return {
        "displayModeBar": False,
        "displaylogo": False,
        "responsive": True,
    }


def get_chart_layout() -> dict:
    """Standard layout for professional charts"""
    return {
        "font": {
            "family": FONTS["primary"],
            "size": 12,
            "color": COLORS["text_primary"],
        },
        "paper_bgcolor": "white",
        "plot_bgcolor": "white",
        "margin": {"l": 40, "r": 20, "t": 40, "b": 40},
        "hoverlabel": {
            "bgcolor": "white",
            "font_size": 12,
            "font_family": FONTS["primary"],
        },
    }
