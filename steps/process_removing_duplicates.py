import logging

import streamlit as st
import polars as pl

from utils import config
from utils.data_cleaner import (
    clean_special_chars,
    normalize_whitespace,
    lowercase_columns,
    replace_empty_with,
    drop_null_rows,
    drop_duplicates
)
from utils.ui_utils import get_column_display_name


# def clean_columns(
#         ldf: pl.LazyFrame,
#         columns: list[str],
#         regex_pattern: str,
#         to_lowercase: bool = True,
#         nullify_empty: bool = True,
#         empty_value: str | None = None,
# ) -> pl.LazyFrame:
#     if regex_pattern:
#         ldf = ldf.with_columns([
#             pl.col(col)
#             .cast(str)
#             .str.extract_all(regex_pattern)
#             .map_elements(lambda matches: "".join(matches), return_dtype=pl.String)
#             .alias(col)
#             for col in columns
#         ])
#     if to_lowercase:
#         ldf = lowercase_columns(ldf, columns)
#     if nullify_empty:
#         ldf = replace_empty_with(ldf, value=empty_value)
#     return ldf

def clean_columns(
        ldf: pl.LazyFrame,
        columns: list[str],
        regex_pattern: str,
        to_lowercase: bool = True,
        nullify_empty: bool = True,
        empty_value: str | None = None,
) -> pl.LazyFrame:
    logger = logging.getLogger(config.APP_TITLE)
    logger.info(f"Начало очистки колонок: {columns} с паттерном: {regex_pattern}")

    try:
        if regex_pattern:
            ldf = ldf.with_columns([
                pl.col(col)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .alias(col)
                for col in columns
            ])
            logger.debug(f"Приведены к Utf8 и заменены null для колонок: {columns}")

            ldf = ldf.with_columns([
                pl.col(col)
                .str.extract_all(regex_pattern)
                .list.eval(pl.element().filter(pl.element().str.len_chars() > 0),
                           parallel=True)  # Фильтрация пустых строк
                .list.join("")
                .fill_null("")  # Замена null (пустых списков) на ""
                .cast(pl.Utf8)  # Явное приведение к строке
                .alias(col)
                for col in columns
            ])
            logger.debug(f"Извлечены и объединены данные для колонок: {columns}")

        if to_lowercase:
            logger.debug(f"Приведение к нижнему регистру для колонок: {columns}")
            ldf = lowercase_columns(ldf, columns)

        if nullify_empty:
            logger.debug(f"Замена пустых значений на {empty_value} для колонок: {columns}")
            ldf = replace_empty_with(ldf, value=empty_value)

        # Логирование схемы после обработки
        logger.info("Схема после очистки колонок:")
        for name, dtype in ldf.schema.items():
            logger.info(f"  {name}: {dtype}")

    except Exception as e:
        logger.error(f"Ошибка при очистке колонок {columns}: {e}")
        raise

    logger.info(f"Очистка колонок {columns} завершена")
    return ldf


def apply_column_cleaning_pipeline(
        ldf: pl.LazyFrame,
        columns_metadata: dict,
        to_lowercase: bool = True,
        nullify_empty: bool = True,
        empty_value: str | None = None,
) -> pl.LazyFrame:
    standalone_columns = [
        col for col, meta in columns_metadata.items()
        if meta.get("mode") == "standalone"
    ]

    ldf = clean_special_chars(ldf)
    ldf = normalize_whitespace(ldf)

    for col in standalone_columns:
        meta = columns_metadata[col]
        display_name = meta.get("display_name", col)
        concatenated = meta.get("concatenated")
        selected_patterns = meta.get("selected_patterns", [])
        detector = st.session_state.pattern_detector
        compiled_pattern = detector.combine_regex(selected_patterns)
        pattern = compiled_pattern.pattern if compiled_pattern else ""

        if not concatenated:
            ldf = clean_columns(
                ldf,
                columns=[col],
                regex_pattern=pattern,
                to_lowercase=to_lowercase,
                nullify_empty=nullify_empty,
                empty_value=empty_value
            )
        else:
            source_columns = concatenated["source_columns"]
            separator = concatenated["separator"]

            ldf = clean_columns(
                ldf,
                columns=source_columns,
                regex_pattern=pattern,
                to_lowercase=to_lowercase,
                nullify_empty=nullify_empty,
                empty_value=empty_value
            )
            ldf = ldf.with_columns([
                pl.concat_str([pl.col(col) for col in source_columns], separator=separator).alias(display_name)
            ])

    exclude_columns = [
        col for col, meta in columns_metadata.items()
        if meta.get("mode") == "exclude"
    ]
    if exclude_columns:
        ldf = ldf.drop(exclude_columns)

    return ldf


def run_full_cleaning():
    if "lazy_df" not in st.session_state or st.session_state.lazy_df is None:
        st.warning("Сначала загрузите файл на шаге 1!")
        return

    columns_metadata = st.session_state.columns_data
    ldf = st.session_state.lazy_df

    # Выбор колонок для удаления дубликатов и пустых
    standalone_columns = [
        col for col in st.session_state.df.columns
        if st.session_state.columns_data[col]["mode"] == "standalone"
    ]

    unique_columns = st.multiselect(
        "Выберите столбцы, по которым удалять дубликаты:",
        options=standalone_columns,
        format_func=get_column_display_name
    )

    not_empty_columns = st.multiselect(
        "Выберите столбцы, которые не могут быть пустыми:",
        options=standalone_columns,
        format_func=get_column_display_name
    )

    if st.button("🚀 Начать полный процессинг"):
        with st.spinner("Обработка данных..."):
            logger = logging.getLogger(config.APP_TITLE)
            try:
                ldf = apply_column_cleaning_pipeline(ldf, columns_metadata)

                ldf = drop_duplicates(ldf, unique_columns)

                ldf = drop_null_rows(ldf, not_empty_columns)

                st.session_state.lazy_df = ldf

                # Получаем превью без полной materialization всего датасета
                st.session_state.df = ldf.fetch(1000)

                st.success("Обработка завершена!")
            except Exception as e:
                logger.exception("Ошибка при выполнении полного процессинга")
                st.error(f"Ошибка при выполнении процессинга: {e}")
