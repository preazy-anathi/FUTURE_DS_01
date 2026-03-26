from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.analysis import (
    AnomalyConfig,
    aggregate_time_series,
    compute_kpis,
    compute_rfm,
    detect_anomalies_daily,
    top_countries,
    top_customers,
    top_products,
)
from src.cleaning import compute_peak_times
from src.config import OUTPUTS_PLOTS_DIR, get_input_csv_path
from src.data_io import build_sales_view, load_raw_csv, parse_and_enrich
from src.visualization import (
    make_anomaly_scatter_fig,
    make_daily_revenue_fig,
    make_monthly_revenue_fig,
    make_rfm_segment_fig,
    make_top_bar_fig,
    save_plotly_figure,
)


st.set_page_config(
    page_title="Online Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(show_spinner=False)
def load_sales(include_canceled_in_metrics: bool) -> pd.DataFrame:
    """
    Load and prepare the sales dataset once per toggle.

    Streamlit cache keeps this responsive as filters (country/date range) change.
    """
    input_path = get_input_csv_path(None)
    df_raw = load_raw_csv(input_path)
    df = parse_and_enrich(df_raw)
    df_sales = build_sales_view(df, include_canceled=include_canceled_in_metrics)
    # Time features for filtering + charts.
    # (analysis/plotting can re-add, but adding once speeds up UI.)
    df_sales["Date"] = df_sales["InvoiceDate"].dt.normalize()
    df_sales["Hour"] = df_sales["InvoiceDate"].dt.hour
    df_sales["DayOfWeek"] = df_sales["InvoiceDate"].dt.dayofweek
    df_sales["MonthOfYear"] = df_sales["InvoiceDate"].dt.month
    df_sales["YearMonth"] = df_sales["InvoiceDate"].dt.to_period("M").astype(str)
    return df_sales


def filter_sales(df_sales: pd.DataFrame, selected_countries: list[str], start_date: date, end_date: date) -> pd.DataFrame:
    mask = df_sales["Country"].isin(selected_countries)
    mask &= (df_sales["Date"] >= pd.Timestamp(start_date))
    mask &= (df_sales["Date"] <= pd.Timestamp(end_date))
    return df_sales.loc[mask].copy()


def export_current_charts(charts: list[tuple[str, go.Figure]], suffix: str) -> None:
    OUTPUTS_PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    for name, fig in charts:
        out_path = OUTPUTS_PLOTS_DIR / f"{name}_{suffix}.png"
        try:
            save_plotly_figure(fig, out_path)
        except Exception:
            # If kaleido isn't available or image export fails, the dashboard should remain usable.
            st.warning(f"Could not export chart `{name}`. Check `kaleido` installation.")


def main() -> None:
    st.title("Online Retail Analytics Dashboard")

    with st.sidebar:
        st.header("Filters")
        include_canceled_in_metrics = st.checkbox("Include canceled invoices in metrics", value=False)
        top_n = st.slider("Top N", min_value=5, max_value=25, value=15, step=1)

        df_sales = load_sales(include_canceled_in_metrics)

        countries = sorted(df_sales["Country"].unique().tolist())
        default_countries = countries  # show everything by default
        selected_countries = st.multiselect("Country", countries, default=default_countries)

        min_d = df_sales["Date"].min().date()
        max_d = df_sales["Date"].max().date()
        start_d, end_d = st.date_input(
            "Date range",
            value=(min_d, max_d),
        )

        if isinstance(start_d, tuple) or isinstance(end_d, tuple):
            # Streamlit can sometimes return tuples depending on version; normalize defensively.
            start_d = start_d[0]  # type: ignore[index]
            end_d = end_d[0]  # type: ignore[index]

    df_filtered = filter_sales(df_sales, selected_countries, start_d, end_d)
    if df_filtered.empty:
        st.warning("No data found for the selected filters.")
        return

    # KPIs and time series
    kpis = compute_kpis(df_filtered)
    st.metric("Total Revenue", f"{kpis['total_revenue']:.2f}")
    st.metric("Total Orders", f"{kpis['total_orders']:,}")
    st.metric("Unique Customers", f"{kpis['total_customers']:,}")
    st.metric("Avg Order Value", f"{kpis['avg_order_value']:.2f}")

    daily = aggregate_time_series(df_filtered, freq="daily", by_country=True)
    monthly = aggregate_time_series(df_filtered, freq="monthly", by_country=True)

    top_c = top_countries(df_filtered, top_n=top_n)
    top_prod = top_products(df_filtered, top_n=top_n)
    top_cust = top_customers(df_filtered, top_n=top_n, by_country=False)

    rfm_customers, rfm_segments = compute_rfm(df_filtered)
    peaks = compute_peak_times(df_filtered)
    daily_full, anomalies_only = detect_anomalies_daily(
        df_filtered,
        AnomalyConfig(window=30, z_threshold=3.0),
    )
    anomalies_small = anomalies_only.copy().sort_values("z_score", ascending=False).head(300)

    tabs = st.tabs(
        [
            "Overview",
            "Revenue Trends",
            "Top Entities",
            "RFM Segments",
            "Peak Times",
            "Anomalies",
        ]
    )

    with tabs[0]:
        st.subheader("Snapshot")
        st.dataframe(
            pd.DataFrame(
                [
                    {"Metric": "Total Revenue", "Value": kpis["total_revenue"]},
                    {"Metric": "Total Orders", "Value": kpis["total_orders"]},
                    {"Metric": "Unique Customers", "Value": kpis["total_customers"]},
                    {"Metric": "Avg Order Value", "Value": kpis["avg_order_value"]},
                ]
            ),
            use_container_width=True,
        )
        st.caption("Use the tabs to explore time trends, rankings, segmentation, and anomalies.")

    with tabs[1]:
        st.subheader("Revenue Trends")
        fig_daily = make_daily_revenue_fig(daily, title="Daily Revenue and Orders")
        st.plotly_chart(fig_daily, use_container_width=True)
        fig_monthly = make_monthly_revenue_fig(monthly, title="Monthly Revenue")
        st.plotly_chart(fig_monthly, use_container_width=True)

        st.download_button(
            label="Download daily time series (filtered)",
            data=daily.to_csv(index=False).encode("utf-8"),
            file_name="daily_time_series_filtered.csv",
            mime="text/csv",
        )

    with tabs[2]:
        st.subheader("Top Entities")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.plotly_chart(
                make_top_bar_fig(
                    top_c,
                    label_col="Country",
                    value_col="Revenue",
                    title=f"Top Countries (Top {top_n})",
                    max_items=top_n,
                ),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                make_top_bar_fig(
                    top_cust,
                    label_col="CustomerID",
                    value_col="Revenue",
                    title=f"Top Customers (Top {top_n})",
                    max_items=top_n,
                ),
                use_container_width=True,
            )
        with col3:
            st.plotly_chart(
                make_top_bar_fig(
                    top_prod,
                    label_col="StockCode",
                    value_col="Revenue",
                    title=f"Top Products (Top {top_n})",
                    max_items=top_n,
                ),
                use_container_width=True,
            )

        st.dataframe(top_c, use_container_width=True)

    with tabs[3]:
        st.subheader("RFM Segments")
        st.plotly_chart(make_rfm_segment_fig(rfm_segments, title="Customer Segments by RFM"), use_container_width=True)
        st.dataframe(rfm_customers.sort_values("rfm_score", ascending=False).head(200), use_container_width=True)

    with tabs[4]:
        st.subheader("Peak Times (Per Country)")
        st.dataframe(peaks, use_container_width=True)

    with tabs[5]:
        st.subheader("Anomalies in Daily Revenue")
        fig_anom = make_anomaly_scatter_fig(anomalies_small, title="Anomalies (Rolling Z-Score)")
        st.plotly_chart(fig_anom, use_container_width=True)
        st.dataframe(anomalies_small, use_container_width=True)

        st.caption("Anomalies are flagged when daily revenue deviates from its rolling mean for the country.")

    with st.sidebar:
        st.divider()
        st.subheader("Export charts")
        if st.button("Export current charts (PNG)"):
            suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            charts = [
                ("revenue_daily", fig_daily),
                ("revenue_monthly", fig_monthly),
                ("rfm_segments", make_rfm_segment_fig(rfm_segments, title="Customer Segments by RFM")),
                ("anomalies", make_anomaly_scatter_fig(anomalies_small, title="Anomalies (Rolling Z-Score)")),
            ]
            export_current_charts(charts, suffix)
            st.success("Exported to `outputs/plots/` (if `kaleido` is available).")


if __name__ == "__main__":
    main()

