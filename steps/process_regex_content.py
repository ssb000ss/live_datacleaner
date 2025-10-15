import streamlit as st
import polars as pl
from utils import config
from utils.ui_utils import get_column_display_name


def apply_selected_patterns():
    if "df" not in st.session_state or st.session_state.df is None:
        st.error("Ошибка: данные ещё не загружены!")
        return

    if "origin_df" not in st.session_state or st.session_state.origin_df is None:
        st.error("Ошибка: отсутствует исходный DataFrame (origin_df). Перезагрузите файл.")
        return

    if "pattern_detector" not in st.session_state or st.session_state.pattern_detector is None:
        st.error("Ошибка: не инициализирован детектор паттернов.")
        return

    df = st.session_state.df.clone()
    origin_df = st.session_state.origin_df
    modified = False
    detector = st.session_state.pattern_detector

    for column, metadata in st.session_state.columns_data.items():
        try:
            if column not in df.columns:
                continue

            if column not in origin_df.columns:
                st.warning(f"Колонка '{column}' отсутствует в origin_df и будет пропущена.")
                continue

            prev_patterns = metadata.get("prev_selected_patterns", [])
            selected_patterns = metadata.get("selected_patterns", [])

            if prev_patterns != selected_patterns:
                try:
                    regex_pattern = detector.combine_regex(selected_patterns)
                except Exception as e:
                    st.error(f"Ошибка объединения паттернов для колонки '{column}': {e}")
                    continue

                origin_col = origin_df[column].cast(str)

                try:
                    if regex_pattern:
                        pattern_str = regex_pattern.pattern
                        cleaned_col = (
                            origin_col
                            .str.extract_all(pattern_str)
                            .map_elements(lambda matches: "".join(matches), return_dtype=pl.String)
                        )
                    else:
                        cleaned_col = origin_col
                except Exception as e:
                    st.error(f"Ошибка применения регулярных выражений для колонки '{column}': {e}")
                    continue

                try:
                    df = df.with_columns(
                        cleaned_col.alias(column)
                    )
                except Exception as e:
                    st.error(f"Ошибка обновления данных для колонки '{column}': {e}")
                    continue

                metadata["prev_selected_patterns"] = selected_patterns.copy()
                modified = True
        except Exception as e:
            st.error(f"Непредвиденная ошибка при обработке колонки '{column}': {e}")
            continue

    if modified:
        st.session_state.df = df


def step_regex_content():
    if "df" not in st.session_state or st.session_state.df is None:
        st.warning("Сначала загрузите и обработайте файл!")
        return

    st.subheader("🧹 Очистка колонок по регулярным выражениям")

    # Фильтруем только standalone колонки (без KeyError)
    columns_data = st.session_state.get("columns_data", {})
    standalone_columns = [
        col for col in st.session_state.df.columns
        if col in columns_data and columns_data.get(col, {}).get("mode") == "standalone"
    ]

    if not standalone_columns:
        st.info("Нет доступных колонок для обработки.")
        return

    column = st.selectbox(
        "Выберите колонку для очистки",
        standalone_columns,
        format_func=get_column_display_name
    )
    # Ключ для выбранных паттернов. Фолбэк на имя колонки, если hash отсутствует
    key_component = st.session_state.columns_data.get(column, {}).get('hash') or column
    selected_patterns_key = f"selected_patterns_{key_component}"

    if selected_patterns_key not in st.session_state:
        st.session_state[selected_patterns_key] = st.session_state.columns_data.get(column, {}).get("display_patterns", [])

    new_display_patterns = st.multiselect(
        "Обнаруженные паттерны в колонке",
        options=st.session_state.columns_data.get(column, {}).get("detected_display_patterns", []),
        default=st.session_state[selected_patterns_key],
        key=f"detected_patterns_{st.session_state.columns_data[column].get('hash')}"
    )

    if new_display_patterns != st.session_state[selected_patterns_key]:
        prev_selected_patterns = st.session_state.columns_data.get(column, {}).get("selected_patterns", [])
        st.session_state[selected_patterns_key] = new_display_patterns
        try:
            selected_patterns = [k for k, v in config.PATTERN_DISPLAY_MAP_UNICODE.items() if v in new_display_patterns]
        except Exception as e:
            st.error(f"Ошибка сопоставления выбранных паттернов: {e}")
            selected_patterns = []

        # Обновляем метаданные безопасно
        st.session_state.columns_data.setdefault(column, {})
        st.session_state.columns_data[column]["display_patterns"] = new_display_patterns
        st.session_state.columns_data[column]["selected_patterns"] = selected_patterns
        st.session_state.columns_data[column]["prev_selected_patterns"] = prev_selected_patterns

        apply_selected_patterns()
        st.rerun()
