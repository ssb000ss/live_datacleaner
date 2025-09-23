import logging
import json
import hashlib
from pathlib import Path
import gc

import streamlit as st
import polars as pl

from utils.data_cleaner import PatternDetector, short_hash
from utils.ui_utils import show_table
from utils import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)

CACHE_ROOT = Path("analyze_cache")


def analyze_file():
    if "source_file" not in st.session_state or st.session_state.source_file is None:
        st.error("Файл не загружен. Перейдите к шагу 1.")
        return

    st.markdown("# Анализ загруженного файла")
    show_table()

    st.checkbox("Игнорировать кеш", key="ignore_column_cache", value=False)

    if st.button("Анализировать колонки"):
        lazy_df = st.session_state.lazy_df
        source_path = st.session_state.source_file

        file_hash = get_file_hash(source_path)
        cache_path = get_cache_path(source_path)

        if not st.session_state.ignore_column_cache:
            cached = try_load_cached_columns_data(lazy_df, file_hash, cache_path)
            if cached:
                st.session_state.columns_data = cached["columns_data"]
                st.success("✅ Настройки колонок загружены из кеша.")
                return

        if lazy_df is not None:
            success = analyze_columns(lazy_df, st.session_state.pattern_detector, source_path)
            if success:
                save_columns_data(st.session_state.columns_data, file_hash, cache_path)
                st.success("✅ Данные успешно загружены и проанализированы!")
        else:
            st.error("❌ Не удалось загрузить данные.")


def analyze_columns(
        lazy_df: pl.LazyFrame,
        pattern_detector: PatternDetector,
        source_path: Path,
) -> bool:
    columns_data = {}

    logger.info(f"Starting column analysis for file: {source_path}")
    try:
        # Получаем список колонок без materialize всего набора данных
        column_names = list(lazy_df.collect_schema().names())
        total_columns = len(column_names)
        progress_bar = st.progress(0)

        for idx, column in enumerate(column_names, start=1):
            logger.info(f"Analyzing column '{column}' in file: {source_path.name}")

            # Формируем выражения для проверки наличия каждого паттерна в колонке
            try:
                exprs = [
                    pl.col(column).cast(pl.Utf8).str.contains(pattern, literal=False).any().alias(name)
                    for name, pattern in pattern_detector.regex_patterns.items()
                ]
                # Собираем только скалярные результаты (True/False для каждого паттерна)
                result_df = lazy_df.select(exprs).collect(streaming=True)
                detected_patterns = {name for name in result_df.columns if bool(result_df[0, name])}
            except Exception as e:
                logger.error(f"Failed pattern detection for column '{column}': {e}")
                detected_patterns = set()

            sorted_patterns = sorted(detected_patterns)
            columns_data[column] = {
                "hash": short_hash(column),
                "column": column,
                "origin_name": column,
                "display_name": column,
                "detected_patterns": sorted_patterns,
                "prev_selected_patterns": sorted_patterns.copy(),
                "selected_patterns": sorted_patterns.copy(),
                "detected_display_patterns": [
                    config.PATTERN_DISPLAY_MAP_UNICODE.get(p, p) for p in sorted_patterns
                ],
                "display_patterns": [
                    config.PATTERN_DISPLAY_MAP_UNICODE.get(p, p) for p in sorted_patterns
                ],
                "mode": "standalone",
                "concatenated": None,
            }

            # Явно освобождаем временные объекты и чистим GC, чтобы не накапливать память на больших файлах
            del result_df
            gc.collect()

            progress_bar.progress(idx / total_columns)

    except Exception as e:
        logger.error("Error during column analysis:", exc_info=True)
        st.error(f"Ошибка при анализе колонок: {e}")
        return False

    st.session_state.columns_data = columns_data
    logger.info("Column analysis completed. Column data saved to session_state.")

    # Дополнительная очистка памяти после завершения анализа всех колонок
    del columns_data
    gc.collect()
    return True


def get_file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_cache_path(source_path: Path) -> Path:
    """Путь до файла кеша: cache/<имя_файла>/columns_data.json"""
    safe_name = source_path.stem.replace(" ", "_")
    cache_dir = CACHE_ROOT / safe_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "columns_data.json"


def try_load_cached_columns_data(lazy_df: pl.LazyFrame, file_hash: str, cache_path: Path) -> dict | None:
    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)

        cached_columns = set(cache.get("columns_data", {}).keys())
        current_columns = set(lazy_df.collect_schema().names())

        if cache.get("file_hash") == file_hash and cached_columns == current_columns:
            logger.info("Загружен кеш колонок с совпадающим хэшем файла.")
            return cache
        else:
            logger.info("Хэш или структура колонок не совпадают — кеш не используется.")

    except Exception as e:
        logger.warning(f"Ошибка при загрузке кеша: {e}")

    return None


def save_columns_data(columns_data: dict, file_hash: str, cache_path: Path):
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({
                "file_hash": file_hash,
                "columns_data": columns_data
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранён кеш columns_data: {cache_path}")
    except Exception as e:
        logger.warning(f"Ошибка при сохранении кеша: {e}")
