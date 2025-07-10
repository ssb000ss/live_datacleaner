import os
import logging
from pathlib import Path

import streamlit as st
import polars as pl

from utils import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


def rename_columns(ldf: pl.LazyFrame) -> pl.LazyFrame:
    if "columns_data" not in st.session_state:
        raise ValueError("Нет данных о колонках в session_state.columns_data")

    columns_data = st.session_state.columns_data

    included_columns = [
        col for col, data in columns_data.items()
        if data.get("mode") != "exclude" and col in ldf.collect_schema()
    ]

    rename_map = {
        col: columns_data[col]["display_name"]
        for col in included_columns
    }

    renamed_ldf = ldf.select(included_columns).rename(rename_map)

    return renamed_ldf


def step_export_file():
    if "lazy_df" not in st.session_state or st.session_state.lazy_df is None:
        st.error("Нет загруженного файла. Сначала выполните шаг 1.")
        return

    if "source_file" not in st.session_state or st.session_state.source_file is None:
        st.error("Неизвестно имя исходного файла.")
        return

    st.subheader("📤 Экспорт данных")

    export_format = st.selectbox("Выберите формат экспорта:", ["parquet", "csv"])
    base_export_dir = st.text_input("Базовая директория для экспорта:", value="exports")

    if export_format == "parquet":
        max_file_size_mb = st.number_input(
            "Максимальный размер одного файла (МБ):", min_value=10, max_value=1000, value=100, step=10
        )
        compression = st.selectbox("Сжатие Parquet:", ["zstd", "snappy", "gzip", "none"], index=0)

    if st.button("Экспортировать"):
        try:
            original_name = st.session_state.source_file.stem
            target_dir = Path(base_export_dir) / original_name
            target_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Экспортная директория: {target_dir.resolve()}")

            ldf = st.session_state.lazy_df.collect()
            ldf = rename_columns(ldf)

            if export_format == "parquet":
                rows_per_chunk = estimate_rows_per_chunk(ldf, max_file_size_mb)
                num_chunks = (ldf.height + rows_per_chunk - 1) // rows_per_chunk

                for i in range(num_chunks):
                    chunk = ldf.slice(i * rows_per_chunk, rows_per_chunk)
                    filename = target_dir / f"{original_name}_part_{i + 1}.parquet"
                    chunk.write_parquet(
                        filename,
                        compression=None if compression == "none" else compression
                    )
                    logger.info(f"Записан: {filename.name}")
                st.success(f"✅ Данные экспортированы в {num_chunks} .parquet файлов в `{target_dir}`")

            elif export_format == "csv":
                filename = target_dir / f"{original_name}.csv"
                ldf.write_csv(
                    file=filename,
                    separator="|"
                )
                st.success(f"✅ Данные экспортированы в CSV: `{filename}`")

        except Exception as e:
            logger.exception("Ошибка при экспорте")
            st.error(f"Ошибка при экспорте: {e}")


def estimate_rows_per_chunk(df: pl.DataFrame, target_mb: int) -> int:
    # Оцениваем размер 10,000 строк с компрессией
    sample_size = min(10_000, df.height)
    sample = df.head(sample_size)
    sample_path = "/tmp/sample_export.parquet"
    sample.write_parquet(sample_path, compression="zstd")

    size_mb = os.path.getsize(sample_path) / 1024 / 1024
    os.remove(sample_path)

    if size_mb == 0:
        return df.height

    rows_per_mb = sample_size / size_mb
    return max(int(rows_per_mb * target_mb), 1)
