import streamlit as st
from src.airbnb_rfm_pyspark.data.ingestion import download_datasets
from src.airbnb_rfm_pyspark.core.spark_session import get_spark
from src.airbnb_rfm_pyspark.processing.rfm_engine import RFMEngine
from src.airbnb_rfm_pyspark.business.recommendations import MarketingRecommendations
from src.airbnb_rfm_pyspark.core.logger import logger

from components.header import render_header
from components.kpis import render_kpis, render_segment_summary
from components.charts import (
    render_segment_distribution,
    render_ltv_distribution,
    render_rfm_scatter,
    render_neighbourhood_performance,
    render_revenue_composition,
)
from components.recommendations import (
    render_recommendations,
    render_priority_customers,
    render_campaign_simulator,
)
from utils.theme import get_custom_css


# =============================================================================
# PAGE CONFIGURATION
# =============================================================================
st.set_page_config(
    page_title="RFM Customer Segmentation",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Apply custom styling
st.markdown(get_custom_css(), unsafe_allow_html=True)


# =============================================================================
# DATA LOADING
# =============================================================================
@st.cache_data(show_spinner=False)
def load_and_process_data():
    """
    ETL pipeline: Download -> Transform -> Segment
    Cached to avoid recomputation on interactions
    """
    with st.spinner("Loading data..."):
        download_success = download_datasets()
        if not download_success:
            st.error("Failed to download datasets")
            st.stop()

    with st.spinner("Processing RFM segmentation..."):
        spark = get_spark()
        engine = RFMEngine(spark)
        df_rfm, df_neighbourhood = engine.process()

    logger.info(f"Pipeline complete: {len(df_rfm):,} customers processed")
    return df_rfm, df_neighbourhood


# Load data
try:
    df_rfm, df_neighbourhood = load_and_process_data()
except Exception as e:
    st.error(f"Critical error: {e}")
    logger.error(f"Fatal error: {e}")
    st.stop()


# =============================================================================
# MAIN APPLICATION
# =============================================================================

# Header
render_header()

st.markdown("---")

# KPIs
render_kpis(df_rfm)

st.markdown("---")

# Segment Overview Table
render_segment_summary(df_rfm)

st.markdown("---")

# Primary Visualizations
st.markdown("## Segment Analysis")

col1, col2 = st.columns(2)

with col1:
    render_segment_distribution(df_rfm)

with col2:
    render_revenue_composition(df_rfm)

st.markdown("---")

# LTV Analysis
st.markdown("## Customer Value Distribution")
render_ltv_distribution(df_rfm)

st.markdown("---")

# Behavioral Patterns
st.markdown("## Behavioral Patterns")
render_rfm_scatter(df_rfm)

st.markdown("---")

# Geographic Analysis
st.markdown("## Geographic Performance")
render_neighbourhood_performance(df_neighbourhood, top_n=10)

st.markdown("---")

# Marketing Recommendations
render_recommendations(df_rfm, MarketingRecommendations)

st.markdown("---")

# High-Value Customers
render_priority_customers(df_rfm, max_rows=100)

st.markdown("---")

# Campaign Simulator
render_campaign_simulator(df_rfm)

st.markdown("---")

# Footer
st.markdown(
    """
    <div style='text-align: center; color: #999; padding: 2rem; font-size: 0.85rem;'>
        <p>Data Source: <a href="http://insideairbnb.com" target="_blank" style='color: #999;'>Inside Airbnb</a> (September 2025)</p>
    </div>
    """,
    unsafe_allow_html=True,
)
