from __future__ import annotations

from calendar import month_abbr

import pandas as pd


DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
MONTH_LABELS = [month_abbr[i] for i in range(1, 13)]  # Jan..Dec


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived time columns used by the analytics layer.

    Output columns:
    - Date (datetime64[ns] at midnight)
    - Hour (0..23)
    - DayOfWeek (0=Mon)
    - DayLabel (Mon..Sun)
    - MonthOfYear (1..12)
    - MonthLabel (Jan..Dec)
    - YearMonth (YYYY-MM string)
    """
    df = df.copy()
    df["Date"] = df["InvoiceDate"].dt.normalize()
    df["Hour"] = df["InvoiceDate"].dt.hour
    df["DayOfWeek"] = df["InvoiceDate"].dt.dayofweek
    df["DayLabel"] = df["DayOfWeek"].map(lambda x: DAY_LABELS[int(x)])
    df["MonthOfYear"] = df["InvoiceDate"].dt.month
    df["MonthLabel"] = df["MonthOfYear"].map(lambda m: MONTH_LABELS[int(m) - 1])
    df["YearMonth"] = df["InvoiceDate"].dt.to_period("M").astype(str)
    return df


def _peak_per_country(
    df_sales: pd.DataFrame,
    bucket_col: str,
    revenue_col: str = "Revenue",
) -> pd.DataFrame:
    """
    Generic peak extractor: for each country pick the bucket with max revenue.
    """
    agg = df_sales.groupby(["Country", bucket_col], as_index=False)[revenue_col].sum()
    idx = agg.groupby("Country")[revenue_col].idxmax()
    peak = agg.loc[idx, ["Country", bucket_col, revenue_col]].copy()
    peak = peak.rename(columns={bucket_col: "PeakBucket", revenue_col: "PeakRevenue"})
    return peak


def compute_peak_times(df_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-country revenue peak times for:
    - Hour of day
    - Day of week
    - Month of year
    - Year-month bucket
    """
    # Hour peaks
    peak_hour = _peak_per_country(df_sales, "Hour")
    peak_hour["PeakHourLabel"] = peak_hour["PeakBucket"].map(
        lambda h: f"{int(h):02d}:00"
    )
    peak_hour = peak_hour.drop(columns=["PeakBucket"]).rename(
        columns={"PeakRevenue": "PeakHourRevenue"}
    )

    # Day-of-week peaks
    peak_day = _peak_per_country(df_sales, "DayOfWeek")
    peak_day["PeakDayOfWeek"] = peak_day["PeakBucket"]
    peak_day["PeakDayLabel"] = peak_day["PeakDayOfWeek"].map(
        lambda d: DAY_LABELS[int(d)]
    )
    peak_day = peak_day.drop(columns=["PeakBucket"]).rename(
        columns={"PeakRevenue": "PeakDayRevenue"}
    )

    # Month peaks
    peak_month = _peak_per_country(df_sales, "MonthOfYear")
    peak_month["PeakMonthLabel"] = peak_month["PeakBucket"].map(
        lambda m: MONTH_LABELS[int(m) - 1]
    )
    peak_month = peak_month.drop(columns=["PeakBucket"]).rename(
        columns={"PeakRevenue": "PeakMonthRevenue"}
    )

    # Year-month peaks
    peak_ym = _peak_per_country(df_sales, "YearMonth")
    peak_ym = peak_ym.rename(
        columns={
            "PeakBucket": "PeakYearMonthLabel",
            "PeakRevenue": "PeakYearMonthRevenue",
        }
    )

    # Combine
    peaks = (
        peak_hour.merge(peak_day, on="Country", how="left")
        .merge(peak_month, on="Country", how="left")
        .merge(peak_ym, on="Country", how="left")
    )

    peaks = peaks[
        [
            "Country",
            "PeakHourLabel",
            "PeakHourRevenue",
            "PeakDayLabel",
            "PeakDayOfWeek",
            "PeakDayRevenue",
            "PeakMonthLabel",
            "PeakMonthRevenue",
            "PeakYearMonthLabel",
            "PeakYearMonthRevenue",
        ]
    ].sort_values("PeakHourRevenue", ascending=False)

    return peaks

