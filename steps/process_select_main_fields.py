import streamlit as st
from utils.ui_utils import get_column_display_name
from utils import config


def step_select_main_fields():
    """Шаг выбора основных полей"""
    if "df" not in st.session_state or st.session_state.df is None:
        st.error("Сначала загрузите файл на шаге 1!")
        return

    if "load_data" not in st.session_state:
        st.session_state.load_data = {
            "main_info": [],
            "additional_info": [],
        }

    # Отображаем и выбираем ТОЛЬКО standalone-колонки, как в шаге "Обработка названий колонок"
    columns_data = st.session_state.get("columns_data", {})
    df_columns = [
        col for col in st.session_state.df.columns
        if col in columns_data and columns_data[col].get("mode") == "standalone"
    ]

    # Автовыбор по отображаемым названиям до тех пор, пока пользователь сам не изменит выбор
    if not st.session_state.get("main_info_user_set"):
        # Используем текущие отображаемые названия (display_name) для сопоставления
        columns_data = st.session_state.get("columns_data", {})
        target_names = {name.strip().lower() for name in getattr(config, "DEFAULT_COLUMN_NAMES", [])}

        default_main_fields = []
        for col in df_columns:
            display_name = columns_data.get(col, {}).get("display_name", col)
            if display_name.strip().lower() in target_names:
                default_main_fields.append(col)

        # Если нашли стандартные поля, используем их
        if default_main_fields:
            st.session_state.load_data["main_info"] = default_main_fields
            st.session_state.load_data["additional_info"] = [
                col for col in df_columns if col not in default_main_fields
            ]

    st.markdown("## 🧩 Выбор основных колонок")
    st.markdown("Выберите колонки, которые будут основными (корневыми) полями в итоговом JSON.")

    # Выбор основных полей с отображением переименованных названий и auto-update через on_change
    st.multiselect(
        label="Основные (корневые) поля:",
        options=df_columns,
        default=st.session_state.load_data["main_info"],
        format_func=get_column_display_name,
        key="main_info_selection",
        on_change=handle_main_info_change
    )

    st.markdown("### 🔍 Текущее состояние load_data")
    columns_data = st.session_state.get("columns_data", {})

    def to_display(name: str) -> str:
        return columns_data.get(name, {}).get("display_name", name)

    def map_item(item):
        if isinstance(item, str):
            return to_display(item)
        if isinstance(item, dict):
            return {k: [to_display(x) for x in v] for k, v in item.items()}
        return item

    readable = {
        "main_info": [to_display(c) for c in st.session_state.load_data.get("main_info", [])],
        "additional_info": [map_item(i) for i in st.session_state.load_data.get("additional_info", [])]
    }
    st.json(readable, expanded=True)


def handle_main_info_change():
    """Обработчик изменения выбора основных полей"""
    if "main_info_selection" in st.session_state and "df" in st.session_state:
        # Фиксируем, что пользователь вручную изменил выбор
        st.session_state.main_info_user_set = True
        selected = st.session_state.main_info_selection
        df_columns = st.session_state.df.columns

        # Обновляем main_info
        st.session_state.load_data["main_info"] = selected

        # Автоматически обновляем additional_info
        st.session_state.load_data["additional_info"] = [
            col for col in df_columns if col not in selected
        ]
