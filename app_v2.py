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
    process_column_names, process_regex_content
)
from steps.process_dedup_v2 import step_dedup_validation
from steps.process_workflow_v2 import step_save_workflow
from utils import config

if "logger" not in st.session_state:
    st.session_state.logger = init_logger(config.LOG_FOLDER, config.APP_TITLE)

logger = st.session_state.logger

# Шаги для app_v2
STEPS_V2 = [
    "Загрузка файла",
    "Анализ колонок", 
    "Исключение ненужных колонок",
    "Конкатенация колонок",
    "Обработка колонок",
    "Обработка содержимого",
    "Удаление дубликатов и валидация",
    "Сохранение workflow"
]

# Функции шагов
step_functions = {
    0: process_load_file.step_load_file,
    1: process_analyze_file.analyze_file,
    2: process_exclude_columns.step_exclude_columns,
    3: process_concatenate_columns.step_concatenate_columns,
    4: process_column_names.step_process_column_names,
    5: process_regex_content.step_regex_content,
    6: step_dedup_validation,
    7: step_save_workflow,
}


def initialize_session_state():
    """Инициализация состояния приложения"""
    if "initialized" not in st.session_state:
        logger.info("Инициализация приложения...")
        st.session_state.pattern_detector = PatternDetector()
        st.session_state.file_manager = FileManager()
        st.session_state.loader = DataLoader(config.PARQUET_FOLDER)
        st.session_state.initialized = True
        st.session_state.files = {}
        logger.info("Инициализация завершена.")


def main():
    """Главная функция приложения"""
    st.set_page_config(page_title=f"{config.APP_TITLE} v2", page_icon=config.PAGE_ICON)
    load_css(config.CSS_PATH)
    st.title(f"{config.APP_TITLE} v2 - Workflow Builder")
    
    if "initialized" not in st.session_state:
        initialize_session_state()

    current_step = stx.stepper_bar(steps=STEPS_V2)

    step_func = step_functions.get(current_step)
    if step_func:
        step_func()
    
    # Показываем таблицу для всех шагов кроме загрузки
    if "df" in st.session_state and current_step > 0:
        display_table()


if __name__ == "__main__":
    main()