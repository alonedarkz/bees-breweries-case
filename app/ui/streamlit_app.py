from __future__ import annotations

from pathlib import Path

import pandas as pd
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
def load_gold_dataframe(dataset_path: str) -> pd.DataFrame:
    return pd.read_parquet(dataset_path)


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

    df = load_gold_dataframe(str(dataset_path))
    if df.empty:
        st.warning("A camada gold foi encontrada, mas veio vazia.")
        return

    countries = sorted(df["country"].dropna().unique().tolist())
    selected_country = st.sidebar.selectbox("Pais", options=["Todos"] + countries)

    filtered_df = df.copy()
    if selected_country != "Todos":
        filtered_df = filtered_df[filtered_df["country"] == selected_country]

    total_locations = (
        filtered_df[["country", "state", "city"]].drop_duplicates().shape[0]
    )
    total_breweries = int(filtered_df["brewery_count"].sum())
    total_types = filtered_df["brewery_type"].nunique()

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
    metric_col_1.metric("Breweries", f"{total_breweries:,}")
    metric_col_2.metric("Tipos", f"{total_types}")
    metric_col_3.metric("Locais", f"{total_locations:,}")

    breweries_by_type = (
        filtered_df.groupby("brewery_type", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
    )
    breweries_by_state = (
        filtered_df.groupby("state", as_index=False)["brewery_count"]
        .sum()
        .sort_values("brewery_count", ascending=False)
        .head(15)
    )

    chart_col_1, chart_col_2 = st.columns(2)
    with chart_col_1:
        st.subheader("Breweries por tipo")
        st.bar_chart(breweries_by_type.set_index("brewery_type"))
    with chart_col_2:
        st.subheader("Top estados")
        st.bar_chart(breweries_by_state.set_index("state"))

    st.subheader("Detalhe analitico")
    st.dataframe(
        filtered_df.sort_values(
            ["brewery_count", "country", "state", "city", "brewery_type"],
            ascending=[False, True, True, True, True],
        ),
        use_container_width=True,
    )

    st.caption(f"Dataset em uso: {dataset_path}")


if __name__ == "__main__":
    main()
