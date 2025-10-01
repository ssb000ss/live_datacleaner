import logging
import streamlit as st
from pathlib import Path
import json

from utils import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


def get_export_folders():
    """Получает список папок в exports"""
    exports_path = config.EXPORTS_FOLDER
    if not exports_path.exists():
        return []
    
    folders = []
    for item in exports_path.iterdir():
        if item.is_dir():
            folders.append(item)
    return sorted(folders, key=lambda x: x.name, reverse=True)


def check_required_files(folder_path: Path):
    """Проверяет наличие необходимых файлов в папке"""
    required_files = {
        "parquet": None,
        "workflow": None,
        "columns_data": None
    }
    
    # Ищем parquet файл
    for file in folder_path.glob("*.parquet"):
        required_files["parquet"] = file
        break
    
    # Ищем workflow.json
    workflow_file = folder_path / "workflow.json"
    if workflow_file.exists():
        required_files["workflow"] = workflow_file
    
    # Ищем columns_data.json
    columns_file = folder_path / "columns_data.json"
    if columns_file.exists():
        required_files["columns_data"] = columns_file
    
    return required_files


def load_workflow_data(workflow_path: Path):
    """Загружает данные из workflow.json"""
    try:
        with open(workflow_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки workflow: {e}")
        return None


def load_columns_data(columns_path: Path):
    """Загружает данные из columns_data.json"""
    try:
        with open(columns_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки columns_data: {e}")
        return None


def step_select_folder():
    """Шаг 0: Выбор папки с готовыми файлами из exports"""
    st.markdown("# Выбор папки с готовыми файлами")
    st.markdown("Выберите папку из exports, содержащую готовые файлы для обработки.")
    
    # Получаем список папок
    export_folders = get_export_folders()
    
    if not export_folders:
        st.warning("📁 Папка exports пуста или не существует.")
        st.info("Сначала создайте workflow через веб-интерфейс или поместите готовые файлы в папку exports.")
        return
    
    # Выбор папки
    folder_options = {folder.name: folder for folder in export_folders}
    selected_folder_name = st.selectbox(
        "Выберите папку:",
        options=list(folder_options.keys()),
        help="Выберите папку, содержащую готовые файлы для обработки"
    )
    
    if selected_folder_name:
        selected_folder = folder_options[selected_folder_name]
        
        # Проверяем наличие файлов
        st.markdown("### Проверка файлов")
        required_files = check_required_files(selected_folder)
        
        # Отображаем статус файлов
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if required_files["parquet"]:
                st.success(f"✅ Parquet файл найден")
                st.caption(f"📄 {required_files['parquet'].name}")
            else:
                st.error("❌ Parquet файл не найден")
        
        with col2:
            if required_files["workflow"]:
                st.success(f"✅ Workflow найден")
                st.caption(f"⚙️ {required_files['workflow'].name}")
            else:
                st.error("❌ Workflow файл не найден")
        
        with col3:
            if required_files["columns_data"]:
                st.success(f"✅ Columns data найден")
                st.caption(f"📊 {required_files['columns_data'].name}")
            else:
                st.error("❌ Columns data файл не найден")
        
        # Проверяем, что все файлы найдены
        all_files_present = all(required_files.values())
        
        if all_files_present:
            st.success("🎉 Все необходимые файлы найдены!")
            
            if st.button("🚀 Загрузить данные", type="primary"):
                try:
                    # Загружаем данные
                    workflow_data = load_workflow_data(required_files["workflow"])
                    columns_data = load_columns_data(required_files["columns_data"])
                    
                    if workflow_data and columns_data:
                        # Сохраняем в session_state
                        st.session_state.source_folder = selected_folder
                        st.session_state.parquet_file = required_files["parquet"]
                        st.session_state.workflow_file = required_files["workflow"]
                        st.session_state.columns_file = required_files["columns_data"]
                        st.session_state.workflow_data = workflow_data
                        st.session_state.columns_data = columns_data
                        
                        # Загружаем parquet файл
                        import polars as pl
                        lazy_df = pl.scan_parquet(str(required_files["parquet"]))
                        st.session_state.lazy_df = lazy_df
                        st.session_state.origin_df = lazy_df.limit(5000).collect()
                        st.session_state.df = st.session_state.origin_df.clone()
                        
                        st.success("✅ Данные успешно загружены!")
                        st.rerun()
                    else:
                        st.error("❌ Ошибка загрузки данных из файлов")
                        
                except Exception as e:
                    logger.error(f"Ошибка загрузки данных: {e}")
                    st.error(f"❌ Ошибка загрузки данных: {e}")
        else:
            missing_files = [name for name, file in required_files.items() if not file]
            st.error(f"❌ Отсутствуют файлы: {', '.join(missing_files)}")
            st.info("Убедитесь, что в выбранной папке есть все необходимые файлы.")
    
    # Показываем информацию о структуре
    with st.expander("ℹ️ Информация о структуре файлов"):
        st.markdown("""
        **Необходимые файлы в папке:**
        - `*.parquet` - обработанные данные
        - `workflow.json` - настройки обработки
        - `columns_data.json` - метаданные колонок
        
        **Структура папки exports:**
        ```
        exports/
        └── your_project_20240101_120000/
            ├── data.parquet
            ├── workflow.json
            └── columns_data.json
        ```
        """)
