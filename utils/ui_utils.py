from typing import Optional

import streamlit as st
import polars as pl

from pathlib import Path


def get_column_display_name(column: str) -> str:
    return st.session_state.columns_data.get(column, {}).get("display_name", column)


def load_css(file_path):
    with open(file_path, "r") as f:
        css = f.read()
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


def file_selector(folder: Path) -> Optional[Path]:
    if not folder.exists():
        st.error(f"Папки '{folder}' не существует.")
        return None
    if not folder.is_dir():
        st.error(f"Путь '{folder}' не является папкой.")
        return None
    try:
        filenames = [file.name for file in folder.iterdir() if file.is_file()]
    except PermissionError as e:
        st.error(f"Нет доступа к папке: {e}")
        return None
    except OSError as e:
        st.error(f"Ошибка при доступе к папке: {e}")
        return None

    if not filenames:
        st.error("Папка с файлами пуста.")
        return None

    selected_filename = st.selectbox('Выберите файл', filenames)
    return folder / selected_filename


def show_table():
    if (
            "source_file" in st.session_state and
            st.session_state.source_file is not None or
            st.session_state.source_file is not None
    ):
        source_file = st.session_state.source_file
        lazy_df = st.session_state.lazy_df
        
        if lazy_df is None:
            st.error("Данные не загружены")
            return
            
        file_info = {
            "Название": source_file.name,
            "Путь": str(source_file),
            "Кодировка": str(st.session_state.encoding),
            "Разделитель": str(st.session_state.delimiter),
            "Строк (примерно)": str(lazy_df.select(pl.count()).collect()[0, 0]),
            "Колонок": str(len(lazy_df.collect_schema().names())),
        }

        file_info_table = [{"Параметр": k, "Значение": v} for k, v in file_info.items()]
        st.table(file_info_table)


def display_table():
    if "df" not in st.session_state or st.session_state.df is None:
        st.warning("Нет данных для отображения.")
        return

    if "columns_data" not in st.session_state or not st.session_state.columns_data:
        st.warning("Нет метаданных колонок.")
        return

    df = st.session_state.df.clone()
    columns_data = st.session_state.columns_data

    # Определяем колонки, помеченные как standalone
    standalone_columns = [
        col for col in df.columns
        if col in columns_data and columns_data[col].get("mode") == "standalone"
    ]

    if not standalone_columns:
        st.warning("Нет колонок для отображения.")
        return

    # Переименовываем колонки
    renamed_columns = {
        col: columns_data[col].get("display_name", col)
        for col in standalone_columns
    }

    try:
        filtered_df = df.select(standalone_columns)
        renamed_df = filtered_df.rename(renamed_columns)

        st.divider()
        st.dataframe(renamed_df.to_pandas(), use_container_width=True)

    except Exception as e:
        st.error(f"Ошибка отображения таблицы: {e}")
