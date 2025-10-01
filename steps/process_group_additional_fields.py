import streamlit as st
from utils.ui_utils import get_column_display_name


def step_group_additional_fields():
    """Шаг группировки дополнительных полей"""
    if "load_data" not in st.session_state:
        st.error("Сначала выберите основные поля на предыдущем шаге!")
        return

    additional_info = st.session_state.load_data.get("additional_info", [])

    st.markdown("## 🧩 Группировка дополнительных полей")
    st.markdown("Вы можете объединить поля во вложенные объекты внутри `additional_info`.")

    # Получаем плоские поля (строки) из additional_info
    flat_fields = [f for f in additional_info if isinstance(f, str)]
    nested_keys = [k for f in additional_info if isinstance(f, dict) for k in f.keys()]
    existing_keys = set(flat_fields + nested_keys)

    with st.form("group_form", clear_on_submit=True):
        selected_fields = st.multiselect(
            "Выберите поля для вложенного объекта:",
            options=flat_fields,
            format_func=get_column_display_name
        )
        group_key = st.text_input("Ключ вложенного объекта")

        submitted = st.form_submit_button("➕ Добавить группу")

        if submitted:
            if not group_key or not selected_fields:
                st.warning("Введите название ключа и выберите хотя бы одно поле.")
            elif group_key in existing_keys:
                st.error(f"Ключ '{group_key}' уже используется! Выберите другое имя.")
            else:
                # Удаляем выбранные поля из списка
                new_additional_info = [f for f in additional_info if f not in selected_fields]
                new_additional_info.append({group_key: selected_fields})

                st.session_state.load_data["additional_info"] = new_additional_info
                st.rerun()

    st.markdown("---")
    st.markdown("## ❌ Удалить группировку")

    # Показываем существующие группы с возможностью удаления
    for item in additional_info:
        if isinstance(item, dict):
            key = list(item.keys())[0]
            fields = item[key]
            # Покажем элементы группы с отображаемыми именами
            pretty_fields = ", ".join(get_column_display_name(f) for f in fields)
            st.caption(f"Поля: {pretty_fields}")
            if st.button(f"Удалить группу '{key}'"):
                updated = [i for i in additional_info if i != item]
                updated.extend(fields)
                st.session_state.load_data["additional_info"] = updated
                st.rerun()

    st.markdown("### 📦 Текущее состояние `load_data`")
    # Отобразим читаемую версию с display_name для наглядности
    readable = {
        "main_info": [get_column_display_name(c) if isinstance(c, str) else c for c in st.session_state.load_data.get("main_info", [])],
        "additional_info": [
            get_column_display_name(c) if isinstance(c, str) else {k: [get_column_display_name(x) for x in v]}
            for c in st.session_state.load_data.get("additional_info", [])
            for k, v in (c.items() if isinstance(c, dict) else [(None, None)])
        ]
    }
    st.json(readable, expanded=True)

