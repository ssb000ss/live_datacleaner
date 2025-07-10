import hashlib

import streamlit as st
import polars as pl
from utils import config
from utils.ui_utils import get_column_display_name


def apply_selected_patterns():
    if "df" not in st.session_state or st.session_state.df is None:
        st.error("Ошибка: данные ещё не загружены!")
        return

    df = st.session_state.df.clone()
    origin_df = st.session_state.origin_df
    modified = False
    detector = st.session_state.pattern_detector

    for column, metadata in st.session_state.columns_data.items():
        if column not in df.columns:
            continue

        prev_patterns = metadata.get("prev_selected_patterns", [])
        selected_patterns = metadata.get("selected_patterns", [])

        if prev_patterns != selected_patterns:
            regex_pattern = detector.combine_regex(selected_patterns)
            origin_col = origin_df[column].cast(str)

            if regex_pattern:
                pattern_str = regex_pattern.pattern
                cleaned_col = (
                    origin_col
                    .str.extract_all(pattern_str)
                    .map_elements(lambda matches: "".join(matches), return_dtype=pl.String)
                )
            else:
                cleaned_col = origin_col

            df = df.with_columns(
                cleaned_col.alias(column)
            )

            metadata["prev_selected_patterns"] = selected_patterns.copy()
            modified = True

    if modified:
        st.session_state.df = df


def step_regex_content():
    if "df" not in st.session_state or st.session_state.df is None:
        st.warning("Сначала загрузите и обработайте файл!")
        return

    st.subheader("🧹 Очистка колонок по регулярным выражениям")

    # Фильтруем только standalone колонки
    standalone_columns = [
        col for col in st.session_state.df.columns
        if st.session_state.columns_data[col]["mode"] == "standalone"
    ]

    if not standalone_columns:
        st.info("Нет доступных колонок для обработки.")
        return

    column = st.selectbox(
        "Выберите колонку для очистки",
        standalone_columns,
        format_func=get_column_display_name
    )
    selected_patterns_key = f"selected_patterns_{st.session_state.columns_data[column].get('hash')}"

    if selected_patterns_key not in st.session_state:
        st.session_state[selected_patterns_key] = st.session_state.columns_data[column]["display_patterns"]

    new_display_patterns = st.multiselect(
        "Обнаруженные паттерны в колонке",
        options=st.session_state.columns_data[column]["detected_display_patterns"],
        default=st.session_state[selected_patterns_key],
        key=f"detected_patterns_{st.session_state.columns_data[column].get('hash')}"
    )

    if new_display_patterns != st.session_state[selected_patterns_key]:
        prev_selected_patterns = st.session_state.columns_data[column]["selected_patterns"]
        st.session_state[selected_patterns_key] = new_display_patterns
        selected_patterns = [k for k, v in config.PATTERN_DISPLAY_MAP_UNICODE.items() if v in new_display_patterns]
        st.session_state.columns_data[column]["display_patterns"] = new_display_patterns
        st.session_state.columns_data[column]["selected_patterns"] = selected_patterns
        st.session_state.columns_data[column]["prev_selected_patterns"] = prev_selected_patterns
        apply_selected_patterns()
        st.rerun()
