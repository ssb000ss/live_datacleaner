import streamlit as st
import extra_streamlit_components as stx

from utils.data_cleaner import PatternDetector
from utils.data_utils import DataLoader
from utils.file_manager import FileManager
from utils.logger import init_logger
from utils.ui_utils import load_css, display_table
from steps import (
    process_load_file, process_exclude_columns,
    process_analyze_file, process_concatenate_columns,
    process_column_names, process_regex_content,
    process_removing_duplicates, process_export, process_regex_formating
)
from utils import config

if "logger" not in st.session_state:
    st.session_state.logger = init_logger(config.LOG_FOLDER, config.APP_TITLE)

logger = st.session_state.logger

step_functions = {
    0: process_load_file.step_load_file,
    1: process_analyze_file.analyze_file,
    2: process_exclude_columns.step_exclude_columns,
    3: process_concatenate_columns.step_concatenate_columns,
    4: process_column_names.step_process_column_names,
    5: process_regex_content.step_regex_content,
    6: process_regex_formating.step_format_column_values,
    7: process_removing_duplicates.run_full_cleaning,
    8: process_export.step_export_file
}


def initialize_session_state():
    if "initialized" not in st.session_state:
        logger.info("Инициализация приложения...")
        st.session_state.pattern_detector = PatternDetector()
        st.session_state.file_manager = FileManager()
        st.session_state.loader = DataLoader(config.PARQUET_FOLDER)
        st.session_state.initialized = True
        st.session_state.files = {}
        logger.info("Инициализация завершена.")
    else:
        logger.info("Инициализация уже выполнена, пропускаем.")


def main():
    st.set_page_config(page_title=config.APP_TITLE, page_icon=config.PAGE_ICON)
    load_css(config.CSS_PATH)
    st.title(config.APP_TITLE)
    if "initialized" not in st.session_state:
        initialize_session_state()

    current_step = stx.stepper_bar(steps=config.STEPS)

    step_func = step_functions.get(current_step)
    if step_func:
        step_func()
    if "df" in st.session_state and current_step > 1 and current_step != 1:
        display_table()


if __name__ == "__main__":
    main()
