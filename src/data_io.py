from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from .config import (
    COL_COUNTRY,
    COL_CUSTOMER_ID,
    COL_DESCRIPTION,
    COL_INVOICE_DATE,
    COL_INVOICE_NO,
    COL_QUANTITY,
    COL_STOCK_CODE,
    COL_UNIT_PRICE,
)


@dataclass(frozen=True)
class LoadConfig:
    encoding: str = "latin-1"
    include_canceled_in_metrics: bool = False


def load_raw_csv(input_path: Path, encoding: str = "latin-1") -> pd.DataFrame:
    """
    Load the raw CSV with stable dtypes to reduce downstream surprises.
    """
    df = pd.read_csv(
        input_path,
        encoding=encoding,
        dtype={
            COL_INVOICE_NO: "string",
            COL_STOCK_CODE: "string",
            COL_DESCRIPTION: "string",
            COL_CUSTOMER_ID: "string",
            COL_COUNTRY: "string",
        },
    )
    return df


def parse_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse dates/numerics, compute `IsCanceled` and `Revenue`.

    Notes:
    - The original dataset uses InvoiceNo prefixes (e.g., 'C...' for cancels).
    - We coerce invalid numeric/date values to NaN.
    - Rows missing time or revenue inputs are dropped.
    """
    df = df.copy()

    df[COL_INVOICE_DATE] = pd.to_datetime(df[COL_INVOICE_DATE], errors="coerce")
    df[COL_QUANTITY] = pd.to_numeric(df[COL_QUANTITY], errors="coerce")
    df[COL_UNIT_PRICE] = pd.to_numeric(df[COL_UNIT_PRICE], errors="coerce")

    df["IsCanceled"] = df[COL_INVOICE_NO].str.startswith("C", na=False)
    df[COL_COUNTRY] = df[COL_COUNTRY].fillna("Unknown")

    df = df.dropna(subset=[COL_INVOICE_DATE, COL_QUANTITY, COL_UNIT_PRICE]).copy()
    df["Revenue"] = df[COL_QUANTITY] * df[COL_UNIT_PRICE]

    return df


def build_sales_view(
    df: pd.DataFrame,
    include_canceled: bool,
    *,
    quantity_filter: Literal["none", "positive"] = "none",
) -> pd.DataFrame:
    """
    Create the analytics view used for KPIs, RFM, anomalies, and time peaks.

    Parameters:
    - include_canceled: whether canceled rows are included in metric calculations.
    - quantity_filter:
        - "none": keep all rows (including negative quantities that might appear)
        - "positive": keep only Quantity > 0
    """
    sales = df.copy()
    if not include_canceled:
        sales = sales[~sales["IsCanceled"]].copy()

    if quantity_filter == "positive":
        sales = sales[sales[COL_QUANTITY] > 0].copy()

    return sales

