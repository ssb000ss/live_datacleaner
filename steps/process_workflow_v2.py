import streamlit as st
import json
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from utils.data_utils import DataLoader
from utils.filename_utils import generate_nomad_filename, get_current_year
from utils import config


def get_file_hash(path: Path) -> str:
    """Получает хеш файла для проверки изменений"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_workflow_cache_path(source_path: Path) -> Path:
    """Путь до файла workflow кеша"""
    safe_name = source_path.stem.replace(" ", "_")
    cache_dir = Path("workflows") / safe_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "workflow.json"


def create_export_package(source_path: Path, workflow_path: Path, analyze_cache_path: Path) -> Path:
    """Создает папку экспорта с временной меткой и копирует все файлы"""
    # Создаем имя папки с временной меткой
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = source_path.stem.replace(" ", "_")
    export_dir = Path("exports") / f"{safe_name}_{timestamp}"
    export_dir.mkdir(parents=True, exist_ok=True)
    
    # Копируем parquet файл
    parquet_path = st.session_state.loader.get_parquet_path(source_path)
    if parquet_path.exists():
        shutil.copy2(parquet_path, export_dir / f"{safe_name}.parquet")
    
    # Копируем workflow файл
    if workflow_path.exists():
        shutil.copy2(workflow_path, export_dir / "workflow.json")
    
    # Копируем analyze файл
    if analyze_cache_path.exists():
        shutil.copy2(analyze_cache_path, export_dir / "columns_data.json")
    
    return export_dir


def step_save_workflow():
    """8-й шаг: Сохранение workflow для дальнейшего процессинга"""
    if "lazy_df" not in st.session_state or st.session_state.lazy_df is None:
        st.error("Сначала загрузите файл на шаге 1!")
        return

    if "columns_data" not in st.session_state or not st.session_state.columns_data:
        st.error("Сначала проанализируйте файл на шаге 2!")
        return

    st.subheader("💾 Сохранение Workflow")
    
    # Настройки экспорта
    st.markdown("### Настройки экспорта")
    
    col1, col2 = st.columns(2)
    with col1:
        country_code = st.selectbox(
            "Код страны:",
            options=sorted(config.ALLOWED_COUNTRY_CODES),
            index=0,
            help="Код страны для генерации имени файла в формате nomad"
        )
    with col2:
        year = st.number_input(
            "Год:",
            value=get_current_year(),
            min_value=2000,
            max_value=2100,
            help="Год для генерации имени файла"
        )
    
    # Генерируем имя файла для экспорта
    try:
        source_path = st.session_state.source_file
        generated_filename = generate_nomad_filename(
            country_code=country_code,
            original_filename=source_path.name,
            year=year
        )
        st.success(f"📄 Сгенерированное имя файла: `{generated_filename}`")
    except ValueError as e:
        st.error(f"Ошибка генерации имени файла: {e}")
        return
    
    st.info("💡 Нажмите кнопку ниже для сохранения всех настроек в workflow файл.")

    # Простая кнопка сохранения
    if st.button("💾 Сохранить Workflow", type="primary"):
        try:
            # Получаем информацию о файле
            source_path = st.session_state.source_file
            file_hash = get_file_hash(source_path)
            parquet_path = st.session_state.loader.get_parquet_path(source_path)
            
            # Собираем настройки колонок
            columns_data = st.session_state.columns_data
            
            # Определяем режимы колонок
            standalone_columns = [
                col for col, data in columns_data.items()
                if data.get("mode") == "standalone"
            ]
            
            exclude_columns = [
                col for col, data in columns_data.items()
                if data.get("mode") == "exclude"
            ]
            
            # Собираем display names (из анализа колонок + переименования)
            display_names = {}
            
            # Сначала добавляем display names из анализа колонок
            for col, data in columns_data.items():
                if data.get("mode") == "standalone":
                    display_names[col] = data.get("display_name", col)
            
            # Затем добавляем/обновляем переименования из GUI
            if hasattr(st.session_state, 'column_renames') and st.session_state.column_renames:
                display_names.update(st.session_state.column_renames)
            
            # Собираем конкатенации
            concatenations = []
            for col, data in columns_data.items():
                if data.get("concatenated"):
                    concat_info = data["concatenated"]
                    concatenations.append({
                        "name": col,
                        "source_columns": concat_info["source_columns"],
                        "separator": concat_info["separator"]
                    })
            
            # Собираем regex правила
            regex_rules = {}
            for col, data in columns_data.items():
                if data.get("selected_patterns"):
                    regex_rules[col] = data["selected_patterns"]
            
            # Используем настройки экспорта, которые уже выбраны выше
            
            # Настройки экспорта по умолчанию
            export_settings = {
                "format": "parquet",
                "parquet": {
                    "compression": "zstd",
                    "target_mb_per_file": 100
                },
                "csv": {
                    "delimiter": "~",
                    "quote_all": True
                },
                "output_dir": f"exports/{source_path.stem}"
            }
            
            # Создаем упрощенную workflow структуру
            workflow = {
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
                "year": year,
                "country_code": country_code,
                "output_filename": generated_filename,
                "source": {
                    "parquet_path": str(parquet_path),
                    "file_hash": file_hash,
                    "schema": list(st.session_state.lazy_df.collect_schema().names())
                },
                # Новая секция структуры для вложенности
                "structure": {
                    "main_info": st.session_state.get("load_data", {}).get("main_info", []),
                    "additional_info": st.session_state.get("load_data", {}).get("additional_info", [])
                },
                "columns": {
                    "standalone": standalone_columns,
                    "exclude": exclude_columns
                },
                "display_names": display_names,
                "concatenations": concatenations,
                "regex_rules": regex_rules,
                "dedup": {
                    "unique_columns": st.session_state.dedup_settings.get("unique_columns", [])
                },
                "not_empty": {
                    "columns": st.session_state.dedup_settings.get("not_empty_columns", [])
                },
                "export": export_settings
            }

            cache_path = get_workflow_cache_path(source_path)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(workflow, f, ensure_ascii=False, indent=2)
            
            # Создаем папку экспорта с временной меткой
            parquet_path = st.session_state.loader.get_parquet_path(source_path)
            analyze_cache_path = Path("analyze_cache") / source_path.stem / "columns_data.json"
            export_dir = create_export_package(source_path, cache_path, analyze_cache_path)
            
            st.success(f"✅ Workflow сохранен: {cache_path}")
            st.success(f"📦 Пакет экспорта создан: {export_dir}")
            
            # Показываем CLI команду с новыми путями
            cli_command = f"""python cli_process.py \\
    --path {export_dir / f"{source_path.stem}.parquet"} \\
    --analyze_cache {export_dir / "columns_data.json"} \\
    --workflow {export_dir / "workflow.json"}"""
            
            st.code(cli_command, language="bash")
            st.info("💡 Скопируйте команду для запуска CLI процессинга")

        except Exception as e:
            st.error(f"❌ Ошибка сохранения: {e}")
