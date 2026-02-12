import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.theme import SEGMENT_COLORS, get_chart_config, get_chart_layout


def render_segment_distribution(df_rfm: pd.DataFrame):
    """
    Clean segment distribution chart
    """
    segment_counts = df_rfm["segment"].value_counts().reset_index()
    segment_counts.columns = ["segment", "count"]
    segment_counts = segment_counts.sort_values("count", ascending=False)

    fig = go.Figure(
        data=[
            go.Bar(
                x=segment_counts["segment"],
                y=segment_counts["count"],
                marker_color=[
                    SEGMENT_COLORS.get(s, "#767676") for s in segment_counts["segment"]
                ],
                text=segment_counts["count"],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>Customers: %{y:,}<extra></extra>",
            )
        ]
    )

    layout = get_chart_layout()
    layout.update(
        title="Customer Distribution by Segment",
        xaxis_title="",
        yaxis_title="Number of Customers",
        showlegend=False,
        height=350,
    )

    fig.update_layout(layout)

    st.plotly_chart(fig, width="stretch", config=get_chart_config())


def render_ltv_distribution(df_rfm: pd.DataFrame):
    """
    LTV distribution by segment - box plot for variance visibility
    """
    # Order segments by median LTV
    segment_order = (
        df_rfm.groupby("segment")["monetary"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )

    fig = go.Figure()

    for segment in segment_order:
        segment_data = df_rfm[df_rfm["segment"] == segment]["monetary"]

        fig.add_trace(
            go.Box(
                y=segment_data,
                name=segment,
                marker_color=SEGMENT_COLORS.get(segment, "#767676"),
                boxmean=True,
                hovertemplate="<b>%{fullData.name}</b><br>LTV: $%{y:,.0f}<extra></extra>",
            )
        )

    layout = get_chart_layout()
    layout.update(
        title="Lifetime Value Distribution by Segment",
        yaxis_title="Customer LTV ($)",
        xaxis_title="",
        showlegend=False,
        height=350,
    )

    fig.update_layout(layout)

    st.plotly_chart(fig, width="stretch", config=get_chart_config())


def render_rfm_scatter(df_rfm: pd.DataFrame):
    """
    RFM scatter plot - Frequency vs Recency colored by Monetary
    Shows customer clustering patterns
    """

    # Sample if too many points (performance)
    if len(df_rfm) > 5000:
        df_plot = df_rfm.sample(n=5000, random_state=42)
    else:
        df_plot = df_rfm

    fig = px.scatter(
        df_plot,
        x="recency",
        y="frequency",
        color="monetary",
        size="monetary",
        hover_data={
            "segment": True,
            "recency": ":.0f",
            "frequency": ":.0f",
            "monetary": ":$.2f",
        },
        color_continuous_scale="Viridis",
        opacity=0.6,
    )

    layout = get_chart_layout()
    layout.update(
        title="Customer Behavior Patterns (Recency vs Frequency)",
        xaxis_title="Recency (days since last review)",
        yaxis_title="Frequency (number of reviews)",
        height=400,
        coloraxis_colorbar=dict(
            title="LTV ($)",
            thickness=15,
            len=0.7,
        ),
    )

    fig.update_layout(layout)

    st.plotly_chart(fig, width="stretch", config=get_chart_config())


def render_neighbourhood_performance(df_neighbourhood: pd.DataFrame, top_n: int = 10):
    """
    Top performing neighbourhoods
    """
    top_neighbourhoods = df_neighbourhood.head(top_n)

    top_neighbourhoods["favourite_neighbourhood"] = (
        top_neighbourhoods["favourite_neighbourhood"]
        .astype(str)
        .str.encode("latin1", errors="ignore")
        .str.decode("utf-8", errors="ignore")
    )

    fig = go.Figure(
        data=[
            go.Bar(
                y=top_neighbourhoods["favourite_neighbourhood"],
                x=top_neighbourhoods["total_revenue"],
                orientation="h",
                marker_color="#00A699",
                text=top_neighbourhoods["total_revenue"],
                texttemplate="$%{text:,.0f}",
                textposition="outside",
                hovertemplate="<b>%{y}</b><br>Revenue: $%{x:,.0f}<br>Customers: %{customdata[0]:,}<extra></extra>",
                customdata=top_neighbourhoods[["customer_count"]],
            )
        ]
    )

    layout = get_chart_layout()
    layout.update(
        title=f"Top {top_n} Neighbourhoods by Revenue",
        xaxis_title="Total Revenue ($)",
        yaxis_title="",
        height=400,
        yaxis={"categoryorder": "total ascending"},
    )

    fig.update_layout(layout)

    st.plotly_chart(fig, width="stretch", config=get_chart_config())


def render_revenue_composition(df_rfm: pd.DataFrame):
    """
    Revenue composition by segment - shows value concentration
    """
    segment_revenue = (
        df_rfm.groupby("segment")["monetary"]
        .sum()
        .reset_index()
        .sort_values("monetary", ascending=False)
    )

    fig = go.Figure(
        data=[
            go.Pie(
                labels=segment_revenue["segment"],
                values=segment_revenue["monetary"],
                hole=0.4,
                marker_colors=[
                    SEGMENT_COLORS.get(s, "#767676") for s in segment_revenue["segment"]
                ],
                textposition="auto",
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Revenue: $%{value:,.0f}<br>Share: %{percent}<extra></extra>",
            )
        ]
    )

    layout = get_chart_layout()
    layout.update(
        title="Revenue Contribution by Segment",
        height=350,
        showlegend=False,
    )

    fig.update_layout(layout)

    st.plotly_chart(fig, width="stretch", config=get_chart_config())
