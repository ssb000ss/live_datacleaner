import streamlit as st

from utils.ui_utils import get_column_display_name


def step_exclude_columns():
    if "df" not in st.session_state or st.session_state.df is None:
        st.error("Сначала загрузите файл на шаге 1!")
        return

    if "columns_data" not in st.session_state or st.session_state.columns_data is None:
        st.error("Сначала проанализируйте файл на шаге 2!")
        return

    df = st.session_state.df
    all_columns = df.columns

    # Ensure columns_data has entries for all columns to avoid missing-key cases
    for col in all_columns:
        if col not in st.session_state.columns_data:
            st.session_state.columns_data[col] = {"mode": "standalone"}

    current_excluded = [
        col for col in df.columns
        if st.session_state.columns_data.get(col, {}).get("mode") == "exclude"
    ]

    st.multiselect(
        "Выбранные колонки будут исключены из дальнейшей обработки:",
        options=all_columns,
        default=current_excluded,
        key="exclude_columns_selection",
        on_change=handle_column_exclude_change,
        format_func=get_column_display_name
    )


def handle_column_exclude_change():
    selected_list = st.session_state.get("exclude_columns_selection", [])
    selected = set(selected_list)  # O(1) membership checks
    for col in st.session_state.df.columns:
        # Ensure columns_data entry exists for the column
        if col not in st.session_state.columns_data:
            st.session_state.columns_data[col] = {"mode": "standalone"}
        st.session_state.columns_data[col]["mode"] = (
            "exclude" if col in selected else "standalone"
        )
