import streamlit as st
import pandas as pd


def render_recommendations(df_rfm: pd.DataFrame, recommendations_engine):
    """
    Displays actionable marketing recommendations per segment

    Args:
        df_rfm: RFM dataframe
        recommendations_engine: MarketingRecommendations class instance
    """

    st.markdown("### Marketing Actions by Segment")

    # Get unique segments sorted by priority
    segments = df_rfm["segment"].unique()

    # Priority order for display
    priority_order = ["At Risk", "Champions", "Potential", "Loyal", "Hibernating"]
    segments_sorted = [s for s in priority_order if s in segments]

    # Add remaining segments
    for s in segments:
        if s not in segments_sorted:
            segments_sorted.append(s)

    # Create tabs for each segment
    tabs = st.tabs(segments_sorted)

    for idx, segment in enumerate(segments_sorted):
        with tabs[idx]:
            segment_size = len(df_rfm[df_rfm["segment"] == segment])
            recommendation = recommendations_engine.get_recommendations(segment)

            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown(f"**Channel:** {recommendation.channel}")
                st.info(recommendation.message)

                st.markdown("**Suggested Offer:**")
                st.success(recommendation.offer)

            with col2:
                st.metric("Segment Size", f"{segment_size:,}")
                st.metric("Priority", f"Level {recommendation.priority}")
                st.metric("Expected ROI", recommendation.expected_roi_label)


def render_priority_customers(df_rfm: pd.DataFrame, max_rows: int = 50):
    """
    Exportable list of high-value customers

    Args:
        df_rfm: RFM dataframe
        max_rows: Maximum number of rows to display
    """

    st.markdown("### High-Value Customers")

    # Focus on actionable segments
    priority_segments = ["Champions", "At Risk", "Loyal", "Potential"]
    df_priority = df_rfm[df_rfm["segment"].isin(priority_segments)].copy()
    df_priority = df_priority.sort_values("monetary", ascending=False)

    # Add segment filter
    selected_segments = st.multiselect(
        "Filter by segment:",
        options=priority_segments,
        default=priority_segments,
    )

    df_filtered = df_priority[df_priority["segment"].isin(selected_segments)]

    df_filtered["favourite_neighbourhood"] = (
        df_filtered["favourite_neighbourhood"]
        .astype(str)
        .str.encode("latin1", errors="ignore")
        .str.decode("utf-8", errors="ignore")
    )

    # Display table
    st.dataframe(
        df_filtered[
            [
                "reviewer_id",
                "segment",
                "recency",
                "frequency",
                "monetary",
                "favourite_neighbourhood",
            ]
        ].head(max_rows),
        width="stretch",
        hide_index=True,
        column_config={
            "reviewer_id": "Customer ID",
            "segment": st.column_config.TextColumn("Segment"),
            "recency": st.column_config.NumberColumn("Recency (days)", format="%d"),
            "frequency": st.column_config.NumberColumn("Frequency", format="%d"),
            "monetary": st.column_config.NumberColumn("LTV", format="$%.0f"),
            "favourite_neighbourhood": "Preferred Area",
        },
    )

    # Export button
    csv = df_filtered.to_csv(index=False)
    st.download_button(
        label="Export to CSV",
        data=csv,
        file_name="high_value_customers.csv",
        mime="text/csv",
        width="stretch",
    )


def render_campaign_simulator(df_rfm: pd.DataFrame):
    """
    Simple ROI calculator for marketing campaigns

    Args:
        df_rfm: RFM dataframe
    """

    st.markdown("### Campaign Impact Simulator")

    col1, col2, col3 = st.columns(3)

    with col1:
        target_segment = st.selectbox(
            "Target Segment",
            options=["Potential", "At Risk", "Hibernating", "Need Attention"],
        )

    with col2:
        conversion_rate = st.slider(
            "Expected Conversion Rate (%)",
            min_value=1,
            max_value=50,
            value=10,
        )

    with col3:
        avg_uplift = st.number_input(
            "Avg Revenue per Conversion ($)",
            min_value=10,
            max_value=1000,
            value=100,
        )

    # Calculate impact
    segment_data = df_rfm[df_rfm["segment"] == target_segment]
    segment_size = len(segment_data)
    expected_conversions = int(segment_size * (conversion_rate / 100))
    projected_revenue = expected_conversions * avg_uplift

    # Display results
    st.markdown("#### Projected Impact")

    result_col1, result_col2, result_col3 = st.columns(3)

    with result_col1:
        st.metric("Target Audience", f"{segment_size:,}")

    with result_col2:
        st.metric("Expected Conversions", f"{expected_conversions:,}")

    with result_col3:
        st.metric("Projected Revenue", f"${projected_revenue:,.0f}")
