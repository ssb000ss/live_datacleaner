import os
import csv
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

    # Формируем безопасные имена столбцов с учётом возможного отсутствия display_name и дублей
    safe_names = []
    used = set()
    for col in included_columns:
        base_name = columns_data.get(col, {}).get("display_name", col) or col
        name = base_name
        idx = 2
        # обеспечим уникальность имён
        while name in used:
            name = f"{base_name}_{idx}"
            idx += 1
        used.add(name)
        safe_names.append((col, name))

    rename_map = {orig: new for orig, new in safe_names}

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
    elif export_format == "csv":
        csv_delimiter = st.text_input("Разделитель CSV:", value="|")
        csv_quote_all = st.checkbox("Заключать все поля в кавычки", value=True)

    if st.button("Экспортировать"):
        try:
            original_name = st.session_state.source_file.stem
            target_dir = Path(base_export_dir) / original_name
            target_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Экспортная директория: {target_dir.resolve()}")

            # ⚠️ Все действия в ленивом режиме ДО collect()
            ldf = rename_columns(st.session_state.lazy_df)

            st.info("🔄 Загружаем данные в память...")
            df = ldf.collect()
            st.write(f"✅ Загружено строк: {df.height}, колонок: {df.width}")

            if export_format == "parquet":
                rows_per_chunk = estimate_rows_per_chunk(df, max_file_size_mb)
                num_chunks = (df.height + rows_per_chunk - 1) // rows_per_chunk
                st.write(f"🔢 Предполагаемое количество файлов: {num_chunks} (по {rows_per_chunk} строк)")

                for i in range(num_chunks):
                    chunk = df.slice(i * rows_per_chunk, rows_per_chunk)
                    filename = target_dir / f"{original_name}_part_{i + 1}.parquet"
                    chunk.write_parquet(
                        filename,
                        compression=None if compression == "none" else compression
                    )
                    logger.info(f"Записан: {filename.name}")
                st.success(f"✅ Данные экспортированы в {num_chunks} .parquet файлов в `{target_dir}`")

            elif export_format == "csv":
                filename = target_dir / f"{original_name}.csv"

                # Заменяем переводы строк внутри текстовых полей, чтобы строки не переносились
                try:
                    string_cols = [col for col, dtype in df.schema.items() if dtype == pl.Utf8]
                except AttributeError:
                    # Fallback для старых версий polars
                    string_cols = [col for col in df.columns if pl.datatypes.is_utf8(df[col].dtype)]

                if string_cols:
                    df = df.with_columns([
                        pl.col(c)
                        .str.replace_all("\r\n", " ")
                        .str.replace_all("\n", " ")
                        .str.replace_all("\r", " ")
                        .alias(c)
                        for c in string_cols
                    ])

                # Пишем CSV вручную, чтобы обеспечить нужный разделитель и кавычки
                with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.writer(
                        f,
                        delimiter=csv_delimiter,
                        quoting=csv.QUOTE_ALL if csv_quote_all else csv.QUOTE_MINIMAL,
                        lineterminator='\n'
                    )
                    # Заголовок
                    writer.writerow(df.columns)
                    # Строки
                    for row in df.iter_rows():
                        safe_row = ["" if v is None else v for v in row]
                        writer.writerow(safe_row)
                st.success(f"✅ Данные экспортированы в CSV: `{filename}`")

        except Exception as e:
            logger.exception("Ошибка при экспорте")
            st.error(f"Ошибка при экспорте: {e}")


def estimate_rows_per_chunk(df: pl.DataFrame, target_mb: int) -> int:
    try:
        sample_size = min(10_000, df.height)
        sample = df.head(sample_size)

        sample_path = "/tmp/sample_export.parquet"
        sample.write_parquet(sample_path, compression="zstd")
        size_mb = os.path.getsize(sample_path) / 1024 / 1024
        os.remove(sample_path)

        if size_mb == 0:
            return max(df.height // 4, 100_000)  # безопасный дефолт

        rows_per_mb = sample_size / size_mb
        return max(int(rows_per_mb * target_mb), 10_000)  # не меньше 10k

    except Exception as e:
        logger.warning(f"Не удалось оценить размер чанка: {e}")
        return max(df.height // 4, 100_000)  # запасной вариант
