import logging
import streamlit as st
import polars as pl

from utils import config
from utils.ui_utils import get_column_display_name

logger = logging.getLogger(config.APP_TITLE)


def get_pattern_name(pattern_name: str) -> str:
    return config.PATTERN_DISPLAY_MAP_UNICODE.get(pattern_name, '')


def step_format_column_values():
    """
    Форматирует значения в выбранной колонке, заменяя символы по regex-шаблону,
    обновляет lazy_df, df и метаданные в session_state.
    """
    st.subheader("🧩 Форматирование значений в колонках")
    logger.info("Запуск шага форматирования значений в колонках")

    # Проверка наличия данных
    if "lazy_df" not in st.session_state or "columns_data" not in st.session_state:
        logger.warning("Отсутствуют данные в session_state: lazy_df или columns_data")
        st.warning("Сначала загрузите файл и выполните анализ колонок!")
        return

    lazy_df = st.session_state.lazy_df
    columns_data = st.session_state.columns_data

    # Выбор колонки для форматирования
    target_column = st.selectbox(
        "Выберите колонку для форматирования:",
        options=[
            col for col in lazy_df.collect_schema().names()
            if columns_data.get(col, {}).get("mode") == "standalone"
        ],
        format_func=get_column_display_name
    )
    logger.debug(f"Выбрана колонка для форматирования: {target_column}")

    selected_patterns = columns_data.get(target_column, {}).get("selected_patterns", [])

    # Интерфейс для ввода regex и замены
    st.markdown("### 🔁 Заменить один regex на другой или литерал")
    col1, col2, col3 = st.columns(3)
    with col1:
        pattern_to_replace = st.selectbox(
            "Что заменить (regex)",
            options=selected_patterns,
            format_func=get_pattern_name
        )
    with col2:
        replacement_pattern = st.selectbox(
            "Чем заменить (regex)",
            options=[""] + selected_patterns,
            index=0,
            format_func=lambda x: "—" if x == "" else get_pattern_name(x)
        )
    with col3:
        replacement_literal = st.text_input("Или заменить на литерал", value="")

    if st.button("Заменить"):
        try:
            # Проверка корректности regex
            if not pattern_to_replace:
                logger.warning("Пустой regex-шаблон")
                st.error("Пожалуйста, выберите корректный regex-шаблон")
                return

            regex_pattern_to_replace = config.REGEX_PATTERNS_UNICODE.get(pattern_to_replace, '')
            if not regex_pattern_to_replace:
                st.error("Не найден шаблон для замены в конфигурации.")
                return

            # Определяем строку-замену: либо литерал, либо репрезентант второго regex (если однозначный)
            if replacement_literal:
                replacement_value = replacement_literal
            elif replacement_pattern:
                # Пытаемся взять видимый символ из карты отображения ( для пунктуации он буквальный )
                replacement_value = config.PATTERN_DISPLAY_MAP_UNICODE.get(replacement_pattern, "")
            else:
                replacement_value = ""

            # Обновление LazyFrame (лениво, без материализации полного набора)
            lazy_df = lazy_df.with_columns([
                pl.col(target_column)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.replace_all(regex_pattern_to_replace, replacement_value)
                .alias(target_column)
            ])

            logger.debug(
                f"Применена замена '{get_pattern_name(pattern_to_replace)}' на '{replacement_value}' в колонке '{target_column}' для LazyFrame")

            # Синхронизация с DataFrame: используем размер окна, равный текущему origin_df (если есть)
            old_df = st.session_state.get("df")
            origin_df = st.session_state.get("origin_df")
            window_rows = origin_df.height if origin_df is not None else 5000
            new_df = lazy_df.limit(window_rows).collect(streaming=True)
            logger.debug(f"DataFrame обновлён из LazyFrame после замены в колонке '{target_column}', строк: {new_df.height}")

            # Обновление метаданных
            logger.debug(f"Метаданные до обновления: {columns_data.get(target_column, {})}")
            updated_metadata = replace_patterns_in_metadata(
                column_metadata=columns_data.get(target_column, {}),
                pattern_to_replace=pattern_to_replace,
                replacement=(replacement_pattern or replacement_value)
            )
            st.session_state.columns_data[target_column] = updated_metadata

            logger.info(f"Метаданные для колонки '{target_column}' обновлены: {updated_metadata}")

            # Ключ для выбранных паттернов с фолбэком на имя колонки
            key_component = st.session_state.columns_data.get(target_column, {}).get('hash') or target_column
            selected_patterns_key = f"selected_patterns_{key_component}"
            if selected_patterns_key not in st.session_state:
                st.session_state[selected_patterns_key] = st.session_state.columns_data[target_column][
                    "display_patterns"]

            # Обновление session_state
            st.session_state.lazy_df = lazy_df
            # Сохраняем/возвращаем производные или конкатенированные колонки, которых нет в новом окне
            if old_df is not None:
                missing_cols = [c for c in old_df.columns if c not in new_df.columns]
                if missing_cols:
                    new_df = new_df.with_columns([old_df[c] for c in missing_cols])

            st.session_state.df = new_df
            # origin_df не изменяем, чтобы размер окна оставался консистентным для других шагов
            st.session_state[selected_patterns_key] = st.session_state.columns_data[target_column][
                "display_patterns"]

            # Логирование схемы LazyFrame
            logger.info("Схема LazyFrame после замены:")
            for name, dtype in lazy_df.collect_schema().items():
                logger.info(f"  {name}: {dtype}")

            st.success(
                f"✅ Заменено '{pattern_to_replace}' на '{replacement_pattern or replacement_value}' в колонке '{target_column}', метаданные обновлены."
            )
            logger.info(f"Успешно завершена замена в колонке '{target_column}'")

        except Exception as e:
            logger.error(f"Ошибка при замене в колонке '{target_column}': {e}", exc_info=True)
            st.error(f"❌ Ошибка при замене: {e}")


def replace_patterns_in_metadata(
        column_metadata: dict,
        pattern_to_replace: str,
        replacement: str
) -> dict:
    """
    Обновляет списки паттернов в метаданных колонки, заменяя old_pattern на new_pattern.

    Args:
        column_metadata: Словарь с метаданными колонки.
        pattern_to_replace: Regex-шаблон для замены (например, '-').
        replacement: Значение для замены (например, '.').

    Returns:
        dict: Обновлённые метаданные.
    """
    logger = logging.getLogger(config.APP_TITLE)
    logger.debug(f"Обновление метаданных для замены '{pattern_to_replace}' на '{replacement}'")
    logger.debug(f"Метаданные до обновления: {column_metadata}")

    updated_metadata = column_metadata.copy()

    # Маппинг отображаемых символов на их идентификаторы
    pattern_map = {v: k for k, v in config.PATTERN_DISPLAY_MAP_UNICODE.items()}
    old_pattern_id = pattern_map.get(pattern_to_replace, pattern_to_replace)
    new_pattern_id = pattern_map.get(replacement, replacement)

    # Списки паттернов для обновления
    pattern_keys = ["detected_patterns", "prev_selected_patterns", "selected_patterns"]
    for key in pattern_keys:
        if key in updated_metadata:
            patterns = updated_metadata[key].copy()  # Копируем список
            # Добавляем new_pattern_id, если его нет и replacement не пустой
            if new_pattern_id and new_pattern_id not in patterns:
                patterns.append(new_pattern_id)
                logger.debug(f"Добавлен '{new_pattern_id}' в '{key}'")
            # Удаляем old_pattern_id, если он есть
            if old_pattern_id in patterns:
                patterns.remove(old_pattern_id)
                logger.debug(f"Удалён '{old_pattern_id}' из '{key}'")
            updated_metadata[key] = patterns

    # Обновление отображаемых паттернов
    for display_key, base_key in (
            ("detected_display_patterns", "detected_patterns"),
            ("display_patterns", "selected_patterns"),
    ):
        if base_key in updated_metadata:
            updated_metadata[display_key] = [
                config.PATTERN_DISPLAY_MAP_UNICODE.get(p, p)
                for p in updated_metadata[base_key]
            ]
            # Для display_patterns фильтруем, чтобы включать только те, что есть в detected_display_patterns
            if display_key == "display_patterns":
                detected_display = updated_metadata.get("detected_display_patterns", [])
                updated_metadata[display_key] = [p for p in updated_metadata[display_key] if p in detected_display]
            logger.debug(f"Обновлён список '{display_key}': {updated_metadata[display_key]}")

    logger.debug(f"Метаданные после обновления: {updated_metadata}")
    return updated_metadata
