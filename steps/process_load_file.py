import logging
import streamlit as st

from utils import config
from utils.ui_utils import file_selector, show_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


def reset_settings():
    st.session_state.source_file = None
    st.session_state.encoding = None
    st.session_state.delimiter = None
    st.session_state.source_file_hash = None


def step_load_file():
    st.markdown("# Загрузка файла и обработка")
    source_file = file_selector(config.INPUT_FOLDER)

    if source_file and st.session_state.get("last_source_file") != source_file:
        if st.button("Переформатировать в Parquet"):
            logger.info(f"Selected file: {source_file}")
            logger.info("Detecting encoding and delimiter...")

            file_path, encoding, delimiter, extension = st.session_state.file_manager.load_data(source_file)

            if (file_path and encoding and delimiter) and extension in config.COMPATIBLE_EXTENSIONS:
                st.session_state.source_file = file_path
                st.session_state.encoding = encoding
                st.session_state.delimiter = delimiter
                st.session_state.last_source_file = source_file

                with st.spinner("Переформатирование файла в Parquet..."):
                    try:
                        if encoding and delimiter:
                            logger.info("Loading data into LazyFrame")
                            lazy_df = st.session_state.loader.load_data(
                                st.session_state.source_file,
                                st.session_state.encoding,
                                st.session_state.delimiter
                            )
                            st.session_state.lazy_df = lazy_df
                            st.session_state.origin_df = lazy_df.collect().head(1000)
                            st.session_state.df = st.session_state.origin_df.clone()
                            show_table()
                            st.success(f"Файл [{file_path.name}] успешно переформатирован и загружен!")
                    except Exception as e:
                        logger.exception("Error during file processing")
                        st.error(e)
            else:
                logger.warning("Incompatible file or failed to detect parameters")
                reset_settings()
                st.error("Несовместимый файл. Пожалуйста, выберите другой файл.")
