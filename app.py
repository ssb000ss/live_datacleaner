import streamlit as st
import extra_streamlit_components as stx  # Исправленный импорт для stepper_bar
from utils.ui_utils import load_css, display_table
from steps import (
    initialize_session_state,
    step_load_file,
    step_process_column_names,
    step_concatenate_columns,
    step_deduplicate_validate,
    step_export,
    step_regex_content, step_exclude_columns
)
from utils import config


def main():
    st.set_page_config(page_title=config.APP_TITLE, page_icon=config.PAGE_ICON)
    load_css(config.CSS_PATH)
    st.title(config.APP_TITLE)
    initialize_session_state()

    current_step = stx.stepper_bar(steps=config.STEPS)

    if current_step == 0:
        step_load_file()
    elif current_step == 1 and st.session_state.df is not None:
        step_exclude_columns()
    elif current_step == 2 and st.session_state.df is not None:
        step_concatenate_columns()
    elif current_step == 3 and st.session_state.df is not None:
        step_process_column_names()
    elif current_step == 4 and st.session_state.df is not None:
        step_regex_content()
    elif current_step == 5 and st.session_state.df is not None:
        step_deduplicate_validate()
    elif current_step == 6 and st.session_state.df is not None:
        step_export()

    display_table()


if __name__ == "__main__":
    main()
