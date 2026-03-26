from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import plotly.graph_objects as go


def _df_to_html_table(df: pd.DataFrame, *, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "<p><em>No data available.</em></p>"
    view = df.head(max_rows).copy()
    return view.to_html(index=False, border=0, classes="table")


def _fig_to_html_div(fig: go.Figure, *, include_plotlyjs: str = "cdn") -> str:
    """
    Convert a Plotly figure to an embeddable HTML <div>.
    `include_plotlyjs="cdn"` keeps the report file smaller.
    """
    return fig.to_html(full_html=False, include_plotlyjs=include_plotlyjs)


def generate_html_report(
    out_path: Path,
    *,
    title: str,
    dataset_path: Path,
    kpis: dict[str, Any],
    figs: dict[str, go.Figure],
    tables: dict[str, pd.DataFrame],
    notes: Optional[str] = None,
) -> Path:
    """
    Generate a single client-ready HTML report.

    The result is a standalone .html file clients can open in any browser.
    For PDF: open the HTML in Chrome/Edge and use Print -> Save as PDF.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    kpi_cards = "\n".join(
        [
            f"""
            <div class="kpi">
              <div class="kpi-label">{label}</div>
              <div class="kpi-value">{value}</div>
            </div>
            """
            for label, value in [
                ("Total revenue", f"{float(kpis.get('total_revenue', 0.0)):,.2f}"),
                ("Total orders", f"{int(kpis.get('total_orders', 0)):,}"),
                ("Unique customers", f"{int(kpis.get('total_customers', 0)):,}"),
                ("Avg order value", f"{float(kpis.get('avg_order_value', 0.0)):,.2f}"),
            ]
        ]
    )

    # Charts (embed all)
    chart_sections = []
    first = True
    for chart_title, fig in figs.items():
        div = _fig_to_html_div(fig, include_plotlyjs="cdn" if first else False)  # only include once
        first = False
        chart_sections.append(
            f"""
            <section class="card">
              <h2>{chart_title}</h2>
              {div}
            </section>
            """
        )
    charts_html = "\n".join(chart_sections)

    # Tables
    table_sections = []
    for table_title, df in tables.items():
        table_sections.append(
            f"""
            <section class="card">
              <h2>{table_title}</h2>
              <div class="table-wrap">
                {_df_to_html_table(df)}
              </div>
            </section>
            """
        )
    tables_html = "\n".join(table_sections)

    notes_html = f"<p>{notes}</p>" if notes else ""

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #0b1220;
      --card: #111a2e;
      --text: #e8eefc;
      --muted: #a9b6d3;
      --accent: #6ea8fe;
      --border: rgba(255,255,255,0.08);
    }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
      background: linear-gradient(180deg, var(--bg), #070b14);
      color: var(--text);
    }}
    .container {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 28px 18px 60px;
    }}
    header {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      letter-spacing: 0.2px;
    }}
    .meta {{
      color: var(--muted);
      font-size: 13px;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 22px;
    }}
    .kpi {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px 14px;
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
    }}
    .kpi-value {{
      font-size: 20px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px;
    }}
    .card h2 {{
      margin: 0 0 10px;
      font-size: 16px;
      color: var(--text);
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table.table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    table.table th, table.table td {{
      padding: 10px 10px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      white-space: nowrap;
    }}
    table.table th {{
      color: var(--muted);
      font-weight: 600;
    }}
    .note {{
      color: var(--muted);
      font-size: 13px;
      margin-top: 10px;
    }}
    @media (max-width: 900px) {{
      .kpis {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>{title}</h1>
      <div class="meta">
        Generated: {generated_at} &nbsp;|&nbsp;
        Dataset: {dataset_path}
      </div>
      <div class="note">
        PDF tip: open this HTML in Chrome/Edge and use Print → Save as PDF.
      </div>
      {notes_html}
    </header>

    <section class="kpis">
      {kpi_cards}
    </section>

    <div class="grid">
      {charts_html}
      {tables_html}
    </div>
  </div>
</body>
</html>
"""

    out_path.write_text(html, encoding="utf-8")
    return out_path

