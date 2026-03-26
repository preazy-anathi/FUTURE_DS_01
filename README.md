# FUTURE_DS_01 - Online Retail Analytics Portfolio

Professional data-cleaning, exploratory analytics, and an interactive dashboard for the classic “online retail” dataset (with `InvoiceNo`, `StockCode`, `Description`, `CustomerID`, `Country`, `InvoiceDate`, `Quantity`, `UnitPrice`).

## Project Overview

This repository turns a one-off analytics script into a complete, portfolio-ready project:

- Refactored, maintainable Python code (`src/`)
- Advanced analytics: time trends, top entities, customer segmentation (RFM), and anomaly detection
- High-quality visualizations with Plotly
- A Streamlit dashboard with interactive filters and KPI tiles
- Exportable artifacts (`outputs/`) including cleaned exports, CSV summaries, and chart images

## Dataset

The dataset is expected to be a CSV with (at minimum) the columns:

- `InvoiceNo` (string, cancelled rows typically start with `"C"`)
- `StockCode` (string)
- `Description` (string)
- `CustomerID` (string)
- `Country` (string)
- `InvoiceDate` (parseable datetime)
- `Quantity` (numeric)
- `UnitPrice` (numeric)

## Features

### Core analytics

- Revenue trends over time (daily + monthly)
- Top countries, customers, and products
- Customer segmentation using a basic RFM approach (Recency, Frequency, Monetary)
- Anomaly detection on unusual daily revenue patterns using rolling z-scores
- Peak times per country (hour, day-of-week, month-of-year, year-month)

### Dashboard interactivity (Streamlit)

- Filter by `Country`
- Filter by `Date range`
- Toggle whether canceled invoices are included in metrics

## Project Structure

- `data/raw/` - place your `data.csv` here
- `data/processed/` - processed artifacts (generated; usually not committed)
- `src/` - clean, testable analytics modules
- `outputs/` - exported CSVs and plot images (generated; gitignored)
- `main.py` - run analytics pipeline and export artifacts
- `app.py` - Streamlit dashboard
- `data_cleaning.py` - backward-compatible entry point (wraps the pipeline)

## How to Run

### 1) Setup

```bash
pip install -r requirements.txt
```

### 2) Put the dataset in place

Copy your dataset to:

- `data/raw/data.csv`

If your dataset is elsewhere, you can point to it:

- `INPUT_CSV=/path/to/data.csv`

### 3) Run the export pipeline (creates CSVs and PNG images)

```bash
python main.py --include-canceled-in-metrics false
```

Optional:

```bash
python main.py --input-csv "C:/path/to/data.csv"
```

### 4) Launch the dashboard

```bash
streamlit run app.py
```

## Exports / Outputs

After running `main.py`, you will find:

- `outputs/data/*.csv` - KPI inputs and precomputed summaries
- `outputs/plots/*.png` - exported Plotly charts (images)

## Screenshots to Include 

Add screenshots for:

1. Dashboard Overview tab (KPIs + revenue trend)
2. Top Entities tab (top countries/customers/products)
3. RFM Segments tab (segment distribution)
4. Anomalies tab (anomaly scatter + anomaly table)

In this project, the corresponding chart exports live in `outputs/plots/` with descriptive filenames.

