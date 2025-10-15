import streamlit as st


def step_save_parquet():
    """Шаг добавления сохранения в Parquet в workflow"""
    if "load_data" not in st.session_state:
        st.error("Сначала настройте поля на предыдущих шагах!")
        return

    st.markdown("## 💾 Настройка сохранения в Parquet")
    st.markdown("Этот шаг добавляет сохранение в Parquet формате в workflow.")
    st.markdown("Трансформация и сохранение будут выполнены при запуске cli_process или gui_simple.")

    # Показываем текущие настройки
    st.markdown("### 📋 Текущие настройки полей")
    st.json(st.session_state.load_data, expanded=True)
    
    st.info("✅ Настройки сохранения добавлены в workflow!")
    st.markdown("При запуске cli_process или gui_simple будет выполнена трансформация данных и сохранение в Parquet формате с вложенной структурой.")
