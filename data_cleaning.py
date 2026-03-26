"""
Backward-compatible entry point.

The analytics pipeline for this project lives in `main.py`.
This file keeps the old filename so existing workflows keep working:
`python data_cleaning.py`
"""

from main import run_pipeline


if __name__ == "__main__":
    run_pipeline()
    raise SystemExit(0)

"""
Backward-compatible entry point.

The analytics pipeline for this project lives in `main.py`.
This file keeps the old filename so existing workflows keep working:
`python data_cleaning.py`
"""

from main import run_pipeline


if __name__ == "__main__":
    run_pipeline()

"""
Backward-compatible entry point.

The original one-off script has been refactored into a full project.
This file keeps the old name so existing runs like:
`python data_cleaning.py`
still work.
"""

from main import run_pipeline


if __name__ == "__main__":
    run_pipeline()

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# -----------------------
# Config
# -----------------------
INPUT_PATH = Path(r"C:\Users\user\Downloads\data internship\data.csv")
OUT_DIR = INPUT_PATH.parent / "cleaned"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INCLUDE_CANCELED_IN_PEAKS = False  # keep rows in df; this only controls peak calculation
TOP_N_FOR_PLOTS = 15

# -----------------------
# Load
# -----------------------
df = pd.read_csv(
    INPUT_PATH,
    encoding="latin-1",
    dtype={
        "InvoiceNo": "string",
        "StockCode": "string",
        "Description": "string",
        "CustomerID": "string",
        "Country": "string",
    },
)

df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

df["IsCanceled"] = df["InvoiceNo"].str.startswith("C", na=False)
df["Country"] = df["Country"].fillna("Unknown")

# Keep only rows that can contribute to revenue/time
df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"]).copy()
df["Revenue"] = df["Quantity"] * df["UnitPrice"]

# Sales view used ONLY for peaks/graphs
df_sales = df.copy()
if not INCLUDE_CANCELED_IN_PEAKS:
    df_sales = df_sales[~df_sales["IsCanceled"]].copy()

# Optional common filter (comment out if you truly want all quantities)
# df_sales = df_sales[df_sales["Quantity"] > 0].copy()

df_sales["Hour"] = df_sales["InvoiceDate"].dt.hour
df_sales["DayOfWeek"] = df_sales["InvoiceDate"].dt.dayofweek  # 0=Mon
day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
df_sales["DayLabel"] = df_sales["DayOfWeek"].map(lambda x: day_labels[int(x)])

df_sales["MonthOfYear"] = df_sales["InvoiceDate"].dt.month  # 1..12
month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
df_sales["MonthLabel"] = df_sales["MonthOfYear"].map(lambda m: month_labels[int(m)-1])

df_sales["YearMonth"] = df_sales["InvoiceDate"].dt.to_period("M").astype(str)  # "YYYY-MM"
# Example: 2010-12

# -----------------------
# Helper: peak row per country
# -----------------------
def peak_per_country(group_cols, revenue_col="Revenue"):
    """
    Returns a dataframe with:
    - Country
    - PeakBucket (the bucket used in group_cols)
    - PeakRevenue
    """
    # group_cols ends with bucket col
    bucket_col = group_cols[-1]
    agg = df_sales.groupby(group_cols, as_index=False)[revenue_col].sum()

    # idxmax per Country (max revenue for that bucket)
    idx = agg.groupby("Country")[revenue_col].idxmax()
    peak = agg.loc[idx, ["Country", bucket_col, revenue_col]].copy()
    peak = peak.rename(columns={bucket_col: "PeakBucket", revenue_col: "PeakRevenue"})
    return peak

# Hour peaks
peak_hour = peak_per_country(["Country", "Hour"])
peak_hour["PeakHourLabel"] = peak_hour["PeakBucket"].map(lambda h: f"{int(h):02d}:00")
peak_hour = peak_hour.drop(columns=["PeakBucket"]).rename(columns={"PeakRevenue": "PeakHourRevenue"})

# Day-of-week peaks
peak_day = peak_per_country(["Country", "DayOfWeek"])
peak_day = peak_day.merge(
    df_sales[["Country","DayOfWeek","DayLabel"]].drop_duplicates(),
    on=["Country","DayOfWeek"],
    how="left"
)
peak_day["PeakDayOfWeek"] = peak_day["DayOfWeek"]
peak_day = peak_day.drop(columns=["PeakBucket", "DayOfWeek"]).rename(columns={"PeakRevenue": "PeakDayRevenue"})
peak_day = peak_day.rename(columns={"DayLabel": "PeakDayLabel"})

# Month-of-year peaks
peak_month = peak_per_country(["Country", "MonthOfYear"])
peak_month["PeakMonthLabel"] = peak_month["PeakBucket"].map(lambda m: month_labels[int(m)-1])
peak_month = peak_month.drop(columns=["PeakBucket"]).rename(columns={"PeakRevenue": "PeakMonthRevenue"})

# Year-month peaks
peak_ym = peak_per_country(["Country", "YearMonth"])
peak_ym = peak_ym.drop(columns=["PeakBucket"]).rename(columns={"PeakRevenue": "PeakYearMonthRevenue"})
peak_ym = peak_ym.rename(columns={"PeakBucket": "PeakYearMonthLabel"})  # just in case
# Since we dropped PeakBucket above, keep label from YearMonth:
# Recompute label cleanly:
peak_ym = df_sales.groupby(["Country","YearMonth"], as_index=False)["Revenue"].sum()
idx2 = peak_ym.groupby("Country")["Revenue"].idxmax()
peak_ym = peak_ym.loc[idx2, ["Country","YearMonth","Revenue"]].rename(
    columns={"YearMonth":"PeakYearMonthLabel", "Revenue":"PeakYearMonthRevenue"}
)

# Combine all peaks
peaks = (
    peak_hour
    .merge(peak_day, on="Country", how="left")
    .merge(peak_month, on="Country", how="left")
    .merge(peak_ym, on="Country", how="left")
)

# Order columns
peaks = peaks[
    ["Country",
     "PeakHourLabel", "PeakHourRevenue",
     "PeakDayLabel", "PeakDayOfWeek", "PeakDayRevenue",
     "PeakMonthLabel", "PeakMonthRevenue",
     "PeakYearMonthLabel", "PeakYearMonthRevenue"]
].sort_values("PeakHourRevenue", ascending=False)

peaks_csv = OUT_DIR / "peak_times_per_country.csv"
peaks.to_csv(peaks_csv, index=False)
print("Saved:", peaks_csv)

# -----------------------
# Graph helper
# -----------------------
def plot_top_bar(data, value_col, label_col, title, filename):
    top = data.sort_values(value_col, ascending=False).head(TOP_N_FOR_PLOTS).copy()
    top = top.sort_values(value_col, ascending=True)  # nicer horizontal bar ordering

    plt.figure(figsize=(10, 7))
    plt.barh(top["Country"], top[value_col])
    plt.xlabel(value_col)
    plt.title(title)

    # annotate labels at the end of bars
    for i, row in top.reset_index(drop=True).iterrows():
        plt.text(row[value_col], i, f"  {row[label_col]}", va="center")

    plt.tight_layout()
    out_path = OUT_DIR / filename
    plt.savefig(out_path, dpi=200)
    plt.close()
    print("Saved graph:", out_path)

plot_top_bar(
    peaks, "PeakHourRevenue", "PeakHourLabel",
    f"Peak Hour by Country (Top {TOP_N_FOR_PLOTS})",
    "peak_hour_by_country.png"
)

plot_top_bar(
    peaks, "PeakDayRevenue", "PeakDayLabel",
    f"Peak Day of Week by Country (Top {TOP_N_FOR_PLOTS})",
    "peak_day_of_week_by_country.png"
)

plot_top_bar(
    peaks, "PeakMonthRevenue", "PeakMonthLabel",
    f"Peak Month-of-Year by Country (Top {TOP_N_FOR_PLOTS})",
    "peak_month_of_year_by_country.png"
)

plot_top_bar(
    peaks, "PeakYearMonthRevenue", "PeakYearMonthLabel",
    f"Peak Year-Month by Country (Top {TOP_N_FOR_PLOTS})",
    "peak_year_month_by_country.png"
)