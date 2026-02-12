import streamlit as st


def render_header():
    """Renders a clean header"""

    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown(
            """
            <div style='margin-bottom: 1rem;'>
                <h1 style='margin-bottom: 0.25rem;'>Customer Segmentation Dashboard</h1>
                <p style='color: #767676; font-size: 0.95rem; margin: 0;'>
                    RFM Analysis - Airbnb Nothern Basque Country Market
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        # Airbnb logo - subtle placement
        st.markdown(
            """
            <div style='text-align: right; padding-top: 0.5rem;'>
                <img src='https://upload.wikimedia.org/wikipedia/commons/6/69/Airbnb_Logo_B%C3%A9lo.svg' 
                     width='100' style='opacity: 0.7;'/>
            </div>
            """,
            unsafe_allow_html=True,
        )
