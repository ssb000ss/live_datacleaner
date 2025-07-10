import logging
import streamlit as st

from utils import config
from utils.ui_utils import get_column_display_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


def step_process_column_names():
    if "df" not in st.session_state or st.session_state.df is None:
        st.warning("Сначала загрузите файл на шаге 1!")
        return

    st.subheader("Обработка названий колонок")

    standalone_columns = [
        col for col in st.session_state.df.columns
        if st.session_state.columns_data[col]["mode"] == "standalone"
    ]

    if not standalone_columns:
        st.info("Нет доступных колонок для обработки.")
        logger.info("No standalone columns available for display name editing.")
        return

    if "selected_column" not in st.session_state or st.session_state.selected_column not in standalone_columns:
        st.session_state.selected_column = standalone_columns[0]
        logger.info(f"Default column selected for processing: {st.session_state.selected_column}")

    column = st.selectbox(
        "Выберите колонку",
        standalone_columns,
        format_func=get_column_display_name,
        index=standalone_columns.index(st.session_state.selected_column),
        key="selected_column"
    )

    display_name_key = f"display_name_input_{column}"
    current_display_name = st.session_state.columns_data[column]["display_name"]

    new_name = st.text_input(
        "Новое отображаемое название",
        value=current_display_name,
        key=display_name_key,
    )

    st.markdown(
        "**Примечание:** Если это поле основное, рекомендуем выбрать из списка:\n\n"
        f"`{', '.join(config.DEFAULT_COLUMN_NAMES)}`"
    )

    if new_name.strip() and new_name.strip() != current_display_name:
        existing_names = {
            data["display_name"]
            for col, data in st.session_state.columns_data.items()
            if col != column
        }
        if new_name in existing_names:
            logger.warning(f"Attempt to assign duplicate display name: '{new_name}'")
            st.error(f"Название '{new_name}' уже используется.")
        else:
            st.session_state.columns_data[column]["display_name"] = new_name.strip()
            logger.info(f"Display name for column '{column}' changed to '{new_name}'")
            st.success(f"Название колонки обновлено: {column} → {new_name}")
