from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd

from .cleaning import add_time_features


@dataclass(frozen=True)
class AnomalyConfig:
    window: int = 30
    z_threshold: float = 3.0


def compute_kpis(df_sales: pd.DataFrame) -> dict[str, float | int]:
    """
    Compute headline KPIs for the current filtered dataset.
    """
    total_revenue = float(df_sales["Revenue"].sum())
    total_orders = int(df_sales["InvoiceNo"].nunique())
    total_customers = int(df_sales["CustomerID"].nunique())
    avg_order_value = float(total_revenue / total_orders) if total_orders else 0.0
    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "total_customers": total_customers,
        "avg_order_value": avg_order_value,
    }


def aggregate_time_series(
    df_sales: pd.DataFrame,
    *,
    freq: Literal["daily", "monthly"] = "daily",
    by_country: bool = True,
) -> pd.DataFrame:
    """
    Create revenue/orders time-series aggregates.
    """
    df = add_time_features(df_sales)

    if freq == "daily":
        group_cols = ["Date"] + (["Country"] if by_country else [])
        label_col = "Date"
    elif freq == "monthly":
        group_cols = ["YearMonth"] + (["Country"] if by_country else [])
        label_col = "YearMonth"
    else:
        raise ValueError("freq must be 'daily' or 'monthly'")

    grouped = (
        df.groupby(group_cols, as_index=False)
        .agg(
            Revenue=("Revenue", "sum"),
            Orders=("InvoiceNo", "nunique"),
            Customers=("CustomerID", "nunique"),
        )
        .sort_values(label_col)
    )

    return grouped


def top_countries(df_sales: pd.DataFrame, *, top_n: int = 10) -> pd.DataFrame:
    df = add_time_features(df_sales)
    out = (
        df.groupby("Country", as_index=False)
        .agg(
            Revenue=("Revenue", "sum"),
            Orders=("InvoiceNo", "nunique"),
            Customers=("CustomerID", "nunique"),
        )
        .sort_values("Revenue", ascending=False)
        .head(top_n)
    )
    return out


def top_customers(
    df_sales: pd.DataFrame,
    *,
    top_n: int = 10,
    by_country: bool = False,
) -> pd.DataFrame:
    df = df_sales[df_sales["CustomerID"].notna()].copy()
    group_cols = ["CustomerID"] + (["Country"] if by_country else [])

    out = df.groupby(group_cols, as_index=False).agg(
        Revenue=("Revenue", "sum"),
        Orders=("InvoiceNo", "nunique"),
    )
    out["AvgOrderValue"] = out["Revenue"] / out["Orders"].replace({0: np.nan})
    out["AvgOrderValue"] = out["AvgOrderValue"].fillna(0.0)

    out = out.sort_values("Revenue", ascending=False).head(top_n)
    return out


def top_products(df_sales: pd.DataFrame, *, top_n: int = 10) -> pd.DataFrame:
    """
    Top products by revenue.
    """
    # Keep the first non-null description as representative.
    out = (
        df_sales.groupby(["StockCode", "Description"], as_index=False)
        .agg(
            Revenue=("Revenue", "sum"),
            UnitsSold=("Quantity", "sum"),
            Orders=("InvoiceNo", "nunique"),
        )
        .sort_values("Revenue", ascending=False)
        .head(top_n)
    )
    # In case descriptions exist in multiple variations, de-duplicate by StockCode.
    out = out.drop_duplicates(subset=["StockCode"])
    return out


def _rfm_segment_label(r: int, f: int, m: int) -> str:
    """
    Simple RFM segmentation rules (portfolio-friendly, deterministic).

    Quantiles are 1..4 where 4 is "best" for F and M, and "most recent" for R.
    """
    if r == 4 and f == 4 and m == 4:
        return "Champions"
    if r >= 3 and f >= 3 and m >= 3:
        return "Loyal"
    if r == 4 and f <= 2:
        return "New / Recent"
    if r <= 2 and f >= 3:
        return "At Risk"
    if r >= 3 and f <= 2:
        return "Promising"
    return "Others"


def compute_rfm(df_sales: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Basic RFM segmentation:
    - Recency: days since last purchase
    - Frequency: number of orders
    - Monetary: total revenue
    """
    if df_sales.empty:
        empty = pd.DataFrame()
        return empty, empty

    df = df_sales[df_sales["CustomerID"].notna()].copy()
    if df.empty:
        empty = pd.DataFrame()
        return empty, empty

    df = add_time_features(df)
    max_date = df["InvoiceDate"].max()

    rfm = (
        df.groupby("CustomerID", as_index=False)
        .agg(
            last_purchase=("InvoiceDate", "max"),
            frequency=("InvoiceNo", "nunique"),
            monetary=("Revenue", "sum"),
        )
    )

    rfm["recency_days"] = (max_date - rfm["last_purchase"]).dt.days

    def _score_1_to_4(values: pd.Series, *, higher_is_better: bool) -> pd.Series:
        """
        Convert continuous values to an interpretable 1..4 score.
        - Score 4 is best when `higher_is_better=True`
        - Score 4 is best when `higher_is_better=False` (i.e., smaller is better)
        """
        n = len(values)
        if n <= 1:
            base = pd.Series([1] * n, index=values.index, dtype=int)
        else:
            ranks = values.rank(method="first", ascending=True)
            # Normalize ranks to [0..1], where the max maps close to 1.
            perc = (ranks - 1) / (n - 1)
            base = (1 + np.floor(perc * 4)).astype(int)
            base = pd.Series(np.clip(base, 1, 4), index=values.index, dtype=int)

        return base if higher_is_better else (5 - base)

    # Score 4 is "best":
    # - Recency: smaller is better
    # - Frequency & Monetary: higher is better
    rfm["R"] = _score_1_to_4(rfm["recency_days"], higher_is_better=False).astype(int)
    rfm["F"] = _score_1_to_4(rfm["frequency"], higher_is_better=True).astype(int)
    rfm["M"] = _score_1_to_4(rfm["monetary"], higher_is_better=True).astype(int)
    rfm["rfm_score"] = rfm["R"] + rfm["F"] + rfm["M"]

    rfm["segment"] = rfm.apply(
        lambda row: _rfm_segment_label(int(row["R"]), int(row["F"]), int(row["M"])),
        axis=1,
    )

    segment_summary = (
        rfm.groupby("segment", as_index=False)
        .agg(
            customers=("CustomerID", "count"),
            avg_recency_days=("recency_days", "mean"),
            avg_monetary=("monetary", "mean"),
        )
        .sort_values("customers", ascending=False)
    )

    return rfm, segment_summary


def detect_anomalies_daily(
    df_sales: pd.DataFrame,
    config: AnomalyConfig = AnomalyConfig(),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect revenue anomalies using rolling z-scores per country.

    Returns:
    - daily_df: daily aggregates with rolling mean/std and z-scores
    - anomalies: only anomalous rows (abs(z) >= threshold)
    """
    if df_sales.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = add_time_features(df_sales)
    daily = (
        df.groupby(["Country", "Date"], as_index=False)
        .agg(Revenue=("Revenue", "sum"), Orders=("InvoiceNo", "nunique"))
        .sort_values(["Country", "Date"])
    )

    # Rolling stats per country
    daily["rolling_mean"] = daily.groupby("Country")["Revenue"].transform(
        lambda s: s.rolling(window=config.window, min_periods=config.window).mean()
    )
    daily["rolling_std"] = daily.groupby("Country")["Revenue"].transform(
        lambda s: s.rolling(window=config.window, min_periods=config.window).std()
    )

    # Avoid division by zero.
    daily["z_score"] = (daily["Revenue"] - daily["rolling_mean"]) / daily["rolling_std"].replace(
        {0: np.nan}
    )

    daily["is_anomaly"] = daily["z_score"].abs() >= config.z_threshold
    anomalies = daily[daily["is_anomaly"]].copy()
    anomalies["anomaly_type"] = np.where(anomalies["z_score"] > 0, "High revenue", "Low revenue")
    anomalies = anomalies.sort_values(["Country", "Date", "z_score"], ascending=[True, True, False])

    return daily, anomalies

