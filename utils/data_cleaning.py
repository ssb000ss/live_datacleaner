import streamlit as st
import re


def safe_exec(user_code, df):
    """Безопасное выполнение пользовательского кода"""
    local_vars = {"df": df}  # Ограничиваем доступ
    try:
        exec(user_code, {}, local_vars)
        return local_vars.get("df", df)  # Возвращаем измененный df
    except Exception as e:
        st.error(f"Ошибка в коде: {e}")
        return df


def combine_regex(selected_keys: list, regex_dict: dict) -> re.Pattern:
    """Объединяет выбранные регулярные выражения в один объект re.Pattern."""
    patterns = [regex_dict[key] for key in selected_keys if key in regex_dict]
    return re.compile("|".join(patterns)) if patterns else None

