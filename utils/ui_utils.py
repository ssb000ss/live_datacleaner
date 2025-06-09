import streamlit as st
import pandas as pd
from .data_cleaner import PatternDetector
from .config import PATTERN_DISPLAY_MAP


def load_css(file_path):
    with open(file_path, "r") as f:
        css = f.read()
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def data_analyze(df: pd.DataFrame) -> bool:
    for column in df.columns:
        detected_patterns = st.session_state.pattern_detector.detect_patterns(
            df[column].astype(str).tolist())
        st.session_state.columns_data[column] = {
            "column": column,
            "origin_name": column,
            "display_name": column,
            "detected_patterns": list(detected_patterns),
            "prev_selected_patterns": None,
            "selected_patterns": list(detected_patterns),
            "detected_display_patterns": [PATTERN_DISPLAY_MAP.get(p, p) for p in detected_patterns],
            "display_patterns": [PATTERN_DISPLAY_MAP.get(p, p) for p in detected_patterns],
            "mode": "standalone",  # может быть standalone(сам использоваться), excluded(исключен) , included(объединен)
            # "concated" : {
            # "sep": "знак какой то",
            # "columns":["column1", "column2]
            # }
            "concated": None,
        }
    return bool(st.session_state.columns_data)


def apply_selected_patterns():
    if "df" not in st.session_state or st.session_state.df is None:
        st.error("Ошибка: данные еще не загружены!")
        return

    updated_df = st.session_state.origin_df.copy()
    modified = False

    for column, metadata in st.session_state.columns_data.items():
        if column in st.session_state.df.columns:  # Используем column, а не display_name
            prev_patterns = metadata.get("prev_selected_patterns", [])
            selected_patterns = metadata.get("selected_patterns", [])

            if prev_patterns != selected_patterns:
                regex_pattern = st.session_state.pattern_detector.combine_regex(selected_patterns)
                if regex_pattern:
                    updated_df[column] = updated_df[column].fillna("").astype(str).apply(
                        lambda x: "".join(regex_pattern.findall(x)))
                st.session_state.columns_data[column]["prev_selected_patterns"] = selected_patterns.copy()
                modified = True
            else:
                updated_df[column] = st.session_state.df[column]

    if modified:
        st.session_state.df = updated_df


def display_table():
    if st.session_state.df is not None:
        st.divider()
        st.markdown('<div class="outside-table">', unsafe_allow_html=True)

        standalone_columns = [
            column for column in st.session_state.df.columns
            if st.session_state.columns_data[column]["mode"] == "standalone"
        ]

        if not standalone_columns:
            st.warning("Нет колонок для отображения.")
            return

        temp_df = st.session_state.df[standalone_columns].copy()
        renamed_columns = {
            column: st.session_state.columns_data[column]["display_name"]
            for column in standalone_columns
        }
        temp_df.rename(columns=renamed_columns, inplace=True)

        st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
        st.dataframe(temp_df, height=700)
        st.markdown('</div>', unsafe_allow_html=True)
