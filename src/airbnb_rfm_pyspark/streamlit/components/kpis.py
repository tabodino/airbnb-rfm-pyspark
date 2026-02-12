import streamlit as st
import pandas as pd


def render_kpis(df_rfm: pd.DataFrame):
    """
    Displays key business metrics in a clean, scannable format

    Args:
        df_rfm: RFM dataframe with customer segments
    """

    # Calculate metrics
    total_customers = len(df_rfm)
    total_revenue = df_rfm["monetary"].sum()
    avg_ltv = df_rfm["monetary"].mean()
    champions_count = len(df_rfm[df_rfm["segment"] == "Champions"])
    champions_pct = (
        (champions_count / total_customers * 100) if total_customers > 0 else 0
    )

    # Display in clean columns
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Customers",
            value=f"{total_customers:,}",
        )

    with col2:
        st.metric(
            label="Estimated Revenue",
            value=f"${total_revenue:,.0f}",
        )

    with col3:
        st.metric(
            label="Avg Customer LTV",
            value=f"${avg_ltv:.0f}",
        )

    with col4:
        st.metric(
            label="Champions",
            value=f"{champions_count:,}",
            delta=f"{champions_pct:.1f}% of base",
        )


def render_segment_summary(df_rfm: pd.DataFrame):
    """
    Quick segment breakdown table

    Args:
        df_rfm: RFM dataframe
    """

    segment_summary = (
        df_rfm.groupby("segment")
        .agg(
            customers=("reviewer_id", "count"),
            avg_ltv=("monetary", "mean"),
            total_revenue=("monetary", "sum"),
            avg_frequency=("frequency", "mean"),
        )
        .reset_index()
    )

    segment_summary["pct_of_total"] = (
        segment_summary["customers"] / segment_summary["customers"].sum() * 100
    )

    segment_summary = segment_summary.sort_values("total_revenue", ascending=False)

    st.markdown("### Segment Overview")

    st.dataframe(
        segment_summary,
        width="stretch",
        hide_index=True,
        column_config={
            "segment": st.column_config.TextColumn("Segment", width="medium"),
            "customers": st.column_config.NumberColumn(
                "Customers",
                format="%d",
            ),
            "pct_of_total": st.column_config.NumberColumn(
                "% of Base",
                format="%.1f%%",
            ),
            "avg_ltv": st.column_config.NumberColumn(
                "Avg LTV",
                format="$%.0f",
            ),
            "total_revenue": st.column_config.NumberColumn(
                "Total Revenue",
                format="$%.0f",
            ),
            "avg_frequency": st.column_config.NumberColumn(
                "Avg Frequency",
                format="%.1f",
            ),
        },
    )
