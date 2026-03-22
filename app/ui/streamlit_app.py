from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
import streamlit as st

from app.config import GOLD_DIR


def latest_gold_dataset() -> Path | None:
    candidates = sorted(
        [path for path in GOLD_DIR.glob("run_id=*") if path.is_dir()],
        key=lambda path: path.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


@st.cache_data(show_spinner=False)
def load_gold_rows(dataset_path: str) -> list[dict]:
    table = pq.read_table(dataset_path)
    return table.to_pylist()


def unique_countries(rows: list[dict]) -> list[str]:
    countries = {
        str(row.get("country")).strip()
        for row in rows
        if row.get("country") not in (None, "")
    }
    return sorted(countries)


def filter_rows(rows: list[dict], country: str) -> list[dict]:
    if country == "Todos":
        return list(rows)
    return [row for row in rows if row.get("country") == country]


def count_locations(rows: list[dict]) -> int:
    locations = {
        (row.get("country"), row.get("state"), row.get("city"))
        for row in rows
    }
    return len(locations)


def aggregate_rows(rows: list[dict], key_name: str, limit: int | None = None) -> list[dict]:
    totals: dict[str, int] = defaultdict(int)
    for row in rows:
        key = str(row.get(key_name) or "unknown")
        totals[key] += int(row.get("brewery_count", 0) or 0)

    aggregated = [
        {key_name: key, "brewery_count": value}
        for key, value in totals.items()
    ]
    aggregated.sort(key=lambda item: (-item["brewery_count"], item[key_name]))

    if limit is not None:
        return aggregated[:limit]
    return aggregated


def sort_detail_rows(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda row: (
            -(int(row.get("brewery_count", 0) or 0)),
            str(row.get("country") or ""),
            str(row.get("state") or ""),
            str(row.get("city") or ""),
            str(row.get("brewery_type") or ""),
        ),
    )


def main() -> None:
    st.set_page_config(page_title="BEES Breweries Dashboard", layout="wide")
    st.title("BEES Breweries Dashboard")
    st.caption("Visao da camada gold gerada pela pipeline Spark.")

    dataset_path = latest_gold_dataset()
    if dataset_path is None:
        st.warning(
            "Nao achei dados na camada gold ainda. Roda a pipeline primeiro com `docker compose up spark-app --build`."
        )
        return

    rows = load_gold_rows(str(dataset_path))
    if not rows:
        st.warning("A camada gold foi encontrada, mas veio vazia.")
        return

    countries = unique_countries(rows)
    selected_country = st.sidebar.selectbox("Pais", options=["Todos"] + countries)

    filtered_rows = filter_rows(rows, selected_country)

    total_locations = count_locations(filtered_rows)
    total_breweries = sum(int(row.get("brewery_count", 0) or 0) for row in filtered_rows)
    total_types = len({row.get("brewery_type") for row in filtered_rows})

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric("Breweries", f"{total_breweries:,}")
    metric_col_2.metric("Tipos", f"{total_types}")
    metric_col_3.metric("Locais", f"{total_locations:,}")

    breweries_by_type = aggregate_rows(filtered_rows, "brewery_type")
    breweries_by_state = aggregate_rows(filtered_rows, "state", limit=15)

    chart_col_1, chart_col_2 = st.columns(2)
    with chart_col_1:
        st.subheader("Breweries por tipo")
        st.bar_chart(breweries_by_type, x="brewery_type", y="brewery_count")
    with chart_col_2:
        st.subheader("Top estados")
        st.bar_chart(breweries_by_state, x="state", y="brewery_count")

    st.subheader("Detalhe analitico")
    st.dataframe(
        sort_detail_rows(filtered_rows),
        use_container_width=True,
    )

    st.caption(f"Dataset em uso: {dataset_path}")


if __name__ == "__main__":
    main()
