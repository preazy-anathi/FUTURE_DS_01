from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio


def save_plotly_figure(fig: go.Figure, out_path: Path, *, width: int = 1100, height: int = 650) -> None:
    """
    Save a Plotly figure to disk (requires `kaleido`).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # write_image uses kaleido under the hood.
    pio.write_image(fig, str(out_path), width=width, height=height, scale=2)


def make_daily_revenue_fig(df_daily: pd.DataFrame, *, title: str) -> go.Figure:
    """
    Daily revenue + orders using two y-axes.

    Supports:
    - single-country series (one revenue + one orders trace)
    - multi-country series (one revenue + one orders trace per country)
    """
    if df_daily.empty:
        return go.Figure()

    df = df_daily.sort_values("Date")
    has_country = "Country" in df.columns
    if has_country:
        countries = df["Country"].dropna().unique().tolist()
    else:
        countries = []

    fig = go.Figure()

    if has_country and len(countries) > 1:
        # Plot one pair of traces per selected country to avoid cross-country line
        # connections (which would happen if we plotted the raw rows in order).
        for c in countries:
            d = df[df["Country"] == c].sort_values("Date")
            fig.add_trace(
                go.Scatter(
                    x=d["Date"],
                    y=d["Revenue"],
                    mode="lines+markers",
                    name=f"Revenue - {c}",
                    line=dict(width=3),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=d["Date"],
                    y=d["Orders"],
                    mode="lines+markers",
                    name=f"Orders - {c}",
                    line=dict(width=2, dash="dot"),
                    yaxis="y2",
                    showlegend=False,
                )
            )
    else:
        # Single series (either no Country column, or only one country).
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["Revenue"],
                mode="lines+markers",
                name="Revenue",
                line=dict(width=3),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["Orders"],
                mode="lines+markers",
                name="Orders",
                line=dict(width=2, dash="dot"),
                yaxis="y2",
                showlegend=False,
            )
        )

    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Revenue",
        yaxis2=dict(title="Orders", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=60, t=60, b=60),
        template="plotly_white",
    )
    return fig


def make_monthly_revenue_fig(df_monthly: pd.DataFrame, *, title: str) -> go.Figure:
    if df_monthly.empty:
        return go.Figure()

    df = df_monthly.sort_values("YearMonth")
    fig = px.bar(
        df,
        x="YearMonth",
        y="Revenue",
        color="Country" if "Country" in df.columns else None,
        title=title,
    )
    fig.update_layout(
        xaxis_title="Year-Month",
        yaxis_title="Revenue",
        template="plotly_white",
        margin=dict(l=60, r=40, t=60, b=60),
    )
    return fig


def make_top_bar_fig(
    df: pd.DataFrame,
    *,
    label_col: str,
    value_col: str,
    title: str,
    color_col: Optional[str] = None,
    max_items: int = 15,
) -> go.Figure:
    """
    Horizontal bar chart with clean ordering.
    """
    if df.empty:
        return go.Figure()

    d = df.sort_values(value_col, ascending=False).copy()
    d = d.iloc[: min(len(d), max_items)]
    d = d.sort_values(value_col, ascending=True)

    fig = go.Figure()
    if color_col and color_col in d.columns:
        fig = px.bar(
            d,
            x=value_col,
            y=label_col,
            orientation="h",
            color=color_col,
            title=title,
        )
    else:
        fig = px.bar(
            d,
            x=value_col,
            y=label_col,
            orientation="h",
            title=title,
        )

    fig.update_layout(
        xaxis_title=value_col,
        yaxis_title="",
        template="plotly_white",
        margin=dict(l=80, r=30, t=60, b=40),
        showlegend=False,
    )
    return fig


def make_rfm_segment_fig(segment_summary: pd.DataFrame, *, title: str) -> go.Figure:
    if segment_summary.empty:
        return go.Figure()

    fig = px.bar(
        segment_summary,
        x="segment",
        y="customers",
        title=title,
        labels={"customers": "Customers", "segment": "RFM segment"},
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=60, r=40, t=60, b=60),
    )
    return fig


def make_anomaly_scatter_fig(anomalies: pd.DataFrame, *, title: str) -> go.Figure:
    if anomalies.empty:
        return go.Figure()

    df = anomalies.sort_values("Date")
    fig = px.scatter(
        df,
        x="Date",
        y="Revenue",
        color="anomaly_type",
        size="z_score",
        hover_data=["Country", "z_score", "rolling_mean", "rolling_std"],
        title=title,
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=60, r=40, t=60, b=60),
    )
    return fig

