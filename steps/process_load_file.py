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
    # Опция игнорировать Parquet-кеш
    st.checkbox("Игнорировать кеш Parquet (пересоздать .parquet)", key="ignore_parquet_cache", value=False)

    # Сбрасываем блокировку кнопки при выборе нового файла
    if source_file and st.session_state.get("last_source_file") != source_file:
        st.session_state.format_locked = False

    button_disabled = bool(st.session_state.get("format_locked"))

    if source_file and st.session_state.get("last_source_file") != source_file:
        if st.button("Переформатировать в Parquet", disabled=button_disabled):
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
                            # Если включено игнорирование кеша — удаляем существующий parquet перед загрузкой
                            try:
                                if st.session_state.get("ignore_parquet_cache"):
                                    parquet_path = st.session_state.loader.get_parquet_path(st.session_state.source_file)
                                    if parquet_path.exists():
                                        parquet_path.unlink(missing_ok=True)
                                        logger.info(f"Parquet cache removed: {parquet_path}")
                            except Exception as e:
                                logger.warning(f"Failed to clear parquet cache: {e}")

                            lazy_df = st.session_state.loader.load_data(
                                st.session_state.source_file,
                                st.session_state.encoding,
                                st.session_state.delimiter
                            )
                            
                            if lazy_df is None:
                                st.error("Не удалось загрузить файл. Возможно, недостаточно памяти или файл поврежден.")
                                logger.error("Failed to load data - lazy_df is None")
                                return
                            
                            st.session_state.lazy_df = lazy_df
                            st.session_state.origin_df = lazy_df.limit(5000).collect()
                            st.session_state.df = st.session_state.origin_df.clone()
                            show_table()
                            st.success(f"Файл [{file_path.name}] успешно переформатирован и загружен!")
                            st.session_state.format_locked = True
                    except Exception as e:
                        logger.exception("Error during file processing")
                        st.error(e)
            else:
                logger.warning("Incompatible file or failed to detect parameters")
                reset_settings()
                st.error("Несовместимый файл. Пожалуйста, выберите другой файл.")
