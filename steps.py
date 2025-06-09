import streamlit as st

from utils.data_cleaner import PatternDetector
from utils.file_manager import FileManager
from utils.ui_utils import data_analyze, apply_selected_patterns
from utils import config


def initialize_session_state():
    if "file_manager" not in st.session_state:
        st.session_state.file_manager = None
    if "origin_df" not in st.session_state:
        st.session_state.origin_df = None
    if "df" not in st.session_state:
        st.session_state.df = None
    if "full_df" not in st.session_state:
        st.session_state.full_df = None
    if "pattern_detector" not in st.session_state:
        st.session_state.pattern_detector = PatternDetector()
    if "columns_data" not in st.session_state:
        st.session_state.columns_data = {}


def step_load_file():
    data1 = "/Users/dev/Documents/projects/live_datacleaner/data/emias.csv"
    file_path = st.text_input("Введите путь к CSV-файлу:", value=data1)
    if file_path:
        st.session_state.file_manager = FileManager(file_path)
        df, encoding, delimiter = st.session_state.file_manager.load_data()
        if df is not None:
            st.session_state.origin_df = df.copy()
            if data_analyze(dxf):
                st.success(f"Файл загружен! Кодировка: {encoding}, Разделитель: '{delimiter}'")
                if "df" not in st.session_state or st.session_state.df is None:
                    st.session_state.df = st.session_state.origin_df.copy()
        else:
            st.error("Не удалось загрузить файл.")


def step_process_column_names():
    if "df" not in st.session_state or st.session_state.df is None:
        st.warning("Сначала загрузите файл на шаге 1!")
        return
    st.subheader("Обработка колонок")
    col1, col2 = st.columns([2, 2])
    with col1:
        standalone_columns = [
            column for column in st.session_state.df.columns
            if st.session_state.columns_data[column]["mode"] == "standalone"
        ]
        column = st.selectbox(
            "Выберите колонку для очистки",
            standalone_columns
        )

    with col2:
        new_column_name = st.text_input(
            "Новое название колонки",
            value=st.session_state.columns_data[column]["display_name"]
        )
        old_display_name = st.session_state.columns_data[column]["display_name"]
        if new_column_name != old_display_name:
            display_names = [name["display_name"] for name in st.session_state.columns_data.values()]
            if new_column_name in display_names:
                st.error(f"{new_column_name} уже существует!")
                st.session_state.columns_data[column]["display_name"] = old_display_name
            elif not new_column_name.strip():
                st.error("Название колонки не может быть пустым!")
            else:
                st.session_state.columns_data[column]["display_name"] = new_column_name.strip()
                st.success(f"Название изменено: **{column} → {new_column_name}**")

    # 🔥 Показываем примечание, если колонка в списке главных полей
    st.markdown(
        "📝 **Примечание:** Если это поле **главное**, рекомендуем выбрать из списка:\n\n"
        f"🔹 `{', '.join(config.DEFAULT_COLUMN_NAMES)}`"
    )


def step_exclude_columns():
    if "df" not in st.session_state or st.session_state.df is None:
        st.warning("Сначала загрузите файл на шаге 1!")
        return

    st.subheader("Исключение колонок из предобработки документа.")

    # Получаем список колонок, уже помеченных как исключенные
    excluded_list = [
        column for column in st.session_state.df.columns
        if st.session_state.columns_data[column]["mode"] == "exclude"  # Используем одно значение
    ]

    # Выбор колонок для исключения
    exclude_columns = st.multiselect(
        "Выбранные колонки будут исключены из обработки",
        options=st.session_state.df.columns.tolist(),
        default=excluded_list  # Чтобы уже исключенные колонки были выбраны
    )

    if st.button("Исключить колонки", disabled=not exclude_columns):
        # Обновляем режим для колонок
        for column in st.session_state.df.columns:
            if column in exclude_columns:
                st.session_state.columns_data[column]["mode"] = "exclude"
            else:
                st.session_state.columns_data[column]["mode"] = "standalone"


def step_regex_content():
    if "full_df" not in st.session_state or st.session_state.full_df is None:
        st.warning("Сначала загрузите файл на шаге 1!")
        return
    st.subheader("Обработка содержимого колонок по регулярным выражениям.")
    standalone_columns = [
        column for column in st.session_state.full_df.columns
        if st.session_state.columns_data[column]["mode"] == "standalone"
    ]
    column = st.selectbox(
        "Выберите колонку для очистки",
        standalone_columns
    )

    selected_patterns_key = f"selected_patterns_{column}"
    if selected_patterns_key not in st.session_state:
        st.session_state[selected_patterns_key] = st.session_state.columns_data[column]["display_patterns"]

    new_display_patterns = st.multiselect(
        "Обнаруженные паттерны в колонке",
        options=st.session_state.columns_data[column]["detected_display_patterns"],
        default=st.session_state[selected_patterns_key],
        key=f"detected_patterns_{column}"
    )
    if new_display_patterns != st.session_state[selected_patterns_key]:
        prev_selected_patterns = st.session_state.columns_data[column]["selected_patterns"].copy()
        st.session_state[selected_patterns_key] = new_display_patterns
        selected_patterns = [k for k, v in config.PATTERN_DISPLAY_MAP.items() if v in new_display_patterns]
        st.session_state.columns_data[column]["display_patterns"] = new_display_patterns
        st.session_state.columns_data[column]["selected_patterns"] = selected_patterns
        st.session_state.columns_data[column]["prev_selected_patterns"] = prev_selected_patterns
        apply_selected_patterns()
        st.rerun()


def step_concatenate_columns():
    st.subheader("🔗 Объединение колонок")
    standalone_columns = [
        column for column in st.session_state.df.columns
        if st.session_state.columns_data[column]["mode"] == "standalone"
    ]
    # Выбираем колонки для объединения
    columns_to_concat = st.multiselect(
        "Выберите колонки для объединения:",
        options=standalone_columns,
    )

    # Вводим название новой колонки
    concat_name = st.text_input("Название новой колонки:", value="concatenated_column")

    # Выбираем разделитель (по умолчанию - пробел)
    separator = st.text_input("Разделитель:", value=" ")

    # Кнопка объединения (неактивна, если колонки не выбраны)
    if st.button("Объединить колонки", disabled=not columns_to_concat):
        if concat_name in st.session_state.df.columns:
            st.error(f"Колонка '{concat_name}' уже существует!")
        elif columns_to_concat:
            # Объединяем значения с разделителем
            st.session_state.df[concat_name] = st.session_state.df[columns_to_concat] \
                .astype(str) \
                .apply(lambda row: separator.join(row.dropna()), axis=1)

            # 🔥 Обновляем `columns_data`, чтобы колонка учитывалась в дальнейшем анализе
            st.session_state.columns_data[concat_name] = {
                "column": concat_name,
                "origin_name": concat_name,
                "display_name": concat_name,
                "detected_patterns": [],
                "prev_selected_patterns": None,
                "selected_patterns": [],
                "detected_display_patterns": [],
                "display_patterns": [],
                "mode": "standalone",
                "concated": None
            }

            st.success(f"Колонки {columns_to_concat} объединены в '{concat_name}'")


def step_deduplicate_validate():
    st.subheader("Удаление дубликатов и валидация")
    drop_duplicates = st.checkbox("Удалить дубликаты", value=False)
    if drop_duplicates and st.button("Применить удаление дубликатов"):
        initial_rows = len(st.session_state.df)
        st.session_state.df = st.session_state.df.drop_duplicates()
        st.success(f"Удалено {initial_rows - len(st.session_state.df)} дубликатов")

    validate_column = st.selectbox("Выберите колонку для проверки на пустые значения", st.session_state.df.columns)
    if st.button("Проверить"):
        empty_count = st.session_state.df[validate_column].isna().sum()
        st.info(f"В колонке '{validate_column}' найдено {empty_count} пустых значений")


def step_export():
    st.subheader("Экспорт данных")
    export_path = st.text_input("Введите путь для сохранения CSV", value="output.csv")
    if st.button("Экспортировать"):
        st.session_state.df.to_csv(export_path, index=False)
        st.success(f"Данные сохранены в {export_path}")
