from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUTS_DATA_DIR = OUTPUTS_DIR / "data"
OUTPUTS_PLOTS_DIR = OUTPUTS_DIR / "plots"


# Expected columns from the dataset (based on your current script).
COL_INVOICE_NO = "InvoiceNo"
COL_STOCK_CODE = "StockCode"
COL_DESCRIPTION = "Description"
COL_CUSTOMER_ID = "CustomerID"
COL_COUNTRY = "Country"
COL_INVOICE_DATE = "InvoiceDate"
COL_QUANTITY = "Quantity"
COL_UNIT_PRICE = "UnitPrice"


def get_input_csv_path(cli_path: Optional[str] = None) -> Path:
    """
    Resolve the input CSV path in a robust, portfolio-friendly way.

    Resolution order:
    1. `cli_path` if provided
    2. Environment variable `INPUT_CSV`
    3. `data/raw/data.csv` inside this repo
    4. Legacy sibling path: `<repo parent>/data.csv`
    """
    candidates: list[Path] = []

    if cli_path:
        candidates.append(Path(cli_path).expanduser())

    env_path = os.getenv("INPUT_CSV")
    if env_path:
        candidates.append(Path(env_path).expanduser())

    candidates.append(DATA_RAW_DIR / "data.csv")
    candidates.append(PROJECT_ROOT.parent / "data.csv")  # your legacy location

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    raise FileNotFoundError(
        "Could not find the dataset CSV. Put it at `data/raw/data.csv` or set "
        "the environment variable `INPUT_CSV`, or pass `--input-csv` to main.py."
    )


def ensure_output_dirs() -> None:
    """Create output directories for exported CSVs and plot images."""
    OUTPUTS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_PLOTS_DIR.mkdir(parents=True, exist_ok=True)

