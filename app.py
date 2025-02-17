import pandas as pd
import streamlit as st
from annotated_text import annotated_text

from utils.file_utils import load_data
from utils.data_cleaning import safe_exec, combine_regex

st.set_page_config(page_title="Интерактивная очистка CSV", page_icon="📊")


def load_css(file_path):
    with open(file_path, "r") as f:
        css = f.read()
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


load_css("static/style.css")

# Заголовок
st.title("🚀 Интерактивная очистка CSV")

PRESET_REGEX = {
    "Цифры": r"\d+",
    "Спецсимволы": r"[^\w\s]",
    "Удалить пробелы в начале и конце": r"^\s+|\s+$",
    "Оставить только буквы": r"[^a-zA-Zа-яА-ЯёЁ\s]",
}

# Ввод пути к файлу
file_path = st.text_input("Введите путь к CSV-файлу:")

if file_path:
    df = load_data(file_path)
    columns_data = df.columns
    text = st.write('Debug')

    if df is not None:
        st.divider()
        st.subheader("📋 Данные с предустановленными правилами очистки")
        col1, col2, col3 = st.columns([1, 1, 3])

        with col1:
            column = st.selectbox("Выберите колонку для очистки", df.columns)

        if "column_names" not in st.session_state:
            # Изначально заголовки такие же
            st.session_state.column_names = {col: col for col in df.columns}


        with col2:
            new_column_name = st.text_input("Новое название колонки", value=st.session_state.column_names[column])

        with col3:
            st.write("Выберите регулярные выражения:")
            selected_regex_keys = [name for name in PRESET_REGEX if st.checkbox(name, key=f"{column}_{name}")]

        st.session_state.column_names[column] = new_column_name

        if selected_regex_keys:
            regex_pattern = combine_regex(selected_regex_keys, PRESET_REGEX)
            df[column] = df[column].astype(str).str.findall(regex_pattern).str.join("")

        st.divider()
        df = df.rename(columns=st.session_state.column_names)

        st.dataframe(df.head(25), use_container_width=True, height=700)
