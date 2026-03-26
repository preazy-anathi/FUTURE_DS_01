from __future__ import annotations

import argparse
from pathlib import Path

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
from src.cleaning import add_time_features, compute_peak_times
from src.config import OUTPUTS_DATA_DIR, OUTPUTS_PLOTS_DIR, ensure_output_dirs, get_input_csv_path
from src.data_io import build_sales_view, load_raw_csv, parse_and_enrich
from src.visualization import (
    make_anomaly_scatter_fig,
    make_daily_revenue_fig,
    make_monthly_revenue_fig,
    make_rfm_segment_fig,
    make_top_bar_fig,
    save_plotly_figure,
)


def run_pipeline(
    input_csv: str | None = None,
    *,
    include_canceled_in_metrics: bool = False,
    top_n: int = 15,
    anomaly_window: int = 30,
    anomaly_z_threshold: float = 3.0,
) -> None:
    """
    Run the full analytics pipeline and export CSV summaries + PNG charts.
    """
    input_path = get_input_csv_path(input_csv)
    ensure_output_dirs()

    df_raw = load_raw_csv(input_path)
    df = parse_and_enrich(df_raw)
    df_sales = build_sales_view(df, include_canceled=include_canceled_in_metrics)

    # Pre-add time features for consistency across exports.
    df_sales = add_time_features(df_sales)

    # KPIs and time series
    kpis = compute_kpis(df_sales)
    print("KPIs:", kpis)

    daily_by_country = aggregate_time_series(df_sales, freq="daily", by_country=True)
    daily_overall = aggregate_time_series(df_sales, freq="daily", by_country=False)
    monthly_by_country = aggregate_time_series(df_sales, freq="monthly", by_country=True)
    monthly_overall = aggregate_time_series(df_sales, freq="monthly", by_country=False)

    # Top entities
    top_c = top_countries(df_sales, top_n=top_n)
    top_prod = top_products(df_sales, top_n=top_n)
    top_cust = top_customers(df_sales, top_n=top_n)

    # RFM
    rfm_customers, rfm_segments = compute_rfm(df_sales)

    # Peaks
    peaks = compute_peak_times(df_sales)

    # Anomalies
    anomalies_daily, anomalies_only = detect_anomalies_daily(
        df_sales,
        AnomalyConfig(window=anomaly_window, z_threshold=anomaly_z_threshold),
    )

    # Exports (CSV)
    suffix = "canceled_included" if include_canceled_in_metrics else "canceled_excluded"
    OUTPUTS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    daily_by_country.to_csv(OUTPUTS_DATA_DIR / f"daily_time_series_{suffix}.csv", index=False)
    daily_overall.to_csv(OUTPUTS_DATA_DIR / f"daily_time_series_overall_{suffix}.csv", index=False)
    monthly_by_country.to_csv(
        OUTPUTS_DATA_DIR / f"monthly_time_series_{suffix}.csv", index=False
    )
    monthly_overall.to_csv(
        OUTPUTS_DATA_DIR / f"monthly_time_series_overall_{suffix}.csv", index=False
    )

    top_c.to_csv(OUTPUTS_DATA_DIR / f"top_countries_{suffix}.csv", index=False)
    top_cust.to_csv(OUTPUTS_DATA_DIR / f"top_customers_{suffix}.csv", index=False)
    top_prod.to_csv(OUTPUTS_DATA_DIR / f"top_products_{suffix}.csv", index=False)

    rfm_customers.to_csv(OUTPUTS_DATA_DIR / f"rfm_customers_{suffix}.csv", index=False)
    rfm_segments.to_csv(OUTPUTS_DATA_DIR / f"rfm_segments_{suffix}.csv", index=False)

    peaks.to_csv(OUTPUTS_DATA_DIR / f"peaks_{suffix}.csv", index=False)
    anomalies_daily.to_csv(
        OUTPUTS_DATA_DIR / f"anomalies_daily_{suffix}.csv", index=False
    )
    anomalies_only.to_csv(OUTPUTS_DATA_DIR / f"anomalies_only_{suffix}.csv", index=False)

    # Exports (plots)
    OUTPUTS_PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    fig_daily = make_daily_revenue_fig(
        daily_overall,
        title="Revenue Trends (Daily)",
    )
    save_plotly_figure(fig_daily, OUTPUTS_PLOTS_DIR / f"revenue_daily_overall_{suffix}.png")

    fig_monthly = make_monthly_revenue_fig(
        monthly_overall,
        title="Revenue Trends (Monthly)",
    )
    save_plotly_figure(fig_monthly, OUTPUTS_PLOTS_DIR / f"revenue_monthly_overall_{suffix}.png")

    fig_top_c = make_top_bar_fig(
        top_c,
        label_col="Country",
        value_col="Revenue",
        title=f"Top Countries by Revenue (Top {top_n})",
        max_items=top_n,
    )
    save_plotly_figure(fig_top_c, OUTPUTS_PLOTS_DIR / f"top_countries_{suffix}.png")

    fig_rfm = make_rfm_segment_fig(
        rfm_segments,
        title="RFM Segment Distribution",
    )
    save_plotly_figure(fig_rfm, OUTPUTS_PLOTS_DIR / f"rfm_segments_{suffix}.png")

    if not anomalies_only.empty:
        # Keep exports manageable for image generation.
        anomalies_small = anomalies_only.copy().sort_values("z_score", ascending=False).head(200)
        fig_anom = make_anomaly_scatter_fig(
            anomalies_small,
            title="Anomalies in Daily Revenue (Rolling Z-Score)",
        )
        save_plotly_figure(fig_anom, OUTPUTS_PLOTS_DIR / f"anomalies_{suffix}.png")

    print("Exports saved to `outputs/`.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run analytics pipeline and export artifacts.")
    parser.add_argument("--input-csv", type=str, default=None, help="Path to dataset CSV.")
    parser.add_argument(
        "--include-canceled-in-metrics",
        type=str,
        default="false",
        help="Whether to include canceled invoices in metrics (true/false).",
    )
    parser.add_argument("--top-n", type=int, default=15, help="Top-N for charts/tables.")
    parser.add_argument("--anomaly-window", type=int, default=30, help="Rolling window size.")
    parser.add_argument(
        "--anomaly-z-threshold",
        type=float,
        default=3.0,
        help="Z-score threshold for anomaly detection.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    include_canceled = str(args.include_canceled_in_metrics).strip().lower() in {"1", "true", "yes", "y"}
    run_pipeline(
        input_csv=args.input_csv,
        include_canceled_in_metrics=include_canceled,
        top_n=args.top_n,
        anomaly_window=args.anomaly_window,
        anomaly_z_threshold=args.anomaly_z_threshold,
    )

