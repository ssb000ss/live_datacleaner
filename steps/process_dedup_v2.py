import streamlit as st
from utils.ui_utils import get_column_display_name


def step_dedup_validation():
    """7-й шаг: Настройка удаления дубликатов и валидации (без процессинга)"""
    if "lazy_df" not in st.session_state or st.session_state.lazy_df is None:
        st.warning("Сначала загрузите файл на шаге 1!")
        return

    if "columns_data" not in st.session_state or not st.session_state.columns_data:
        st.warning("Сначала проанализируйте файл на шаге 2!")
        return

    # Выбор колонок для валидации
    standalone_columns = [
        col for col in st.session_state.df.columns
        if st.session_state.columns_data[col]["mode"] == "standalone"
    ]

    not_empty_columns = st.multiselect(
        "Выберите столбцы, которые не могут быть пустыми:",
        options=standalone_columns,
        format_func=get_column_display_name
    )

    # Сохраняем настройки в session_state
    if "dedup_settings" not in st.session_state:
        st.session_state.dedup_settings = {}
    
    # Дедупликация по всем колонкам автоматически
    st.session_state.dedup_settings["unique_columns"] = standalone_columns
    st.session_state.dedup_settings["not_empty_columns"] = not_empty_columns
