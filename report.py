from __future__ import annotations

"""
Generate a single client-ready HTML report from the pipeline.

Usage:
  python report.py
  python report.py --input-csv "C:/path/to/data.csv"
"""

import argparse

from main import run_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a client-ready HTML report.")
    parser.add_argument("--input-csv", type=str, default=None, help="Path to dataset CSV.")
    parser.add_argument(
        "--include-canceled-in-metrics",
        type=str,
        default="false",
        help="Whether to include canceled invoices in metrics (true/false).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    include_canceled = str(args.include_canceled_in_metrics).strip().lower() in {"1", "true", "yes", "y"}
    run_pipeline(
        input_csv=args.input_csv,
        include_canceled_in_metrics=include_canceled,
        create_report=True,
    )

