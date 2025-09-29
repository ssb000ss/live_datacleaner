#!/usr/bin/env python3
"""
Упрощенная версия GUI для быстрого тестирования CLI процессора.
"""

import streamlit as st
import subprocess
import json
import os
from pathlib import Path
import tempfile
from datetime import datetime

st.set_page_config(page_title="Simple Data Processor", page_icon="🚀")

st.title("🚀 Simple Data Processor")
st.markdown("Быстрый интерфейс для CLI процессора")

# Параметры
col1, col2 = st.columns(2)

with col1:
    st.subheader("📁 Выбор папки с готовыми файлами")
    
    # Получаем список папок из exports
    exports_path = Path("exports")
    export_folders = []
    
    if exports_path.exists():
        for item in exports_path.iterdir():
            if item.is_dir():
                export_folders.append(item)
        export_folders = sorted(export_folders, key=lambda x: x.name, reverse=True)
    
    if not export_folders:
        st.warning("📁 Папка exports пуста или не существует.")
        st.info("Сначала создайте workflow через веб-интерфейс или поместите готовые файлы в папку exports.")
        input_path = None
        workflow_path = None
        columns_data_path = None
    else:
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
            
            # Ищем файлы
            parquet_files = list(selected_folder.glob("*.parquet"))
            workflow_file = selected_folder / "workflow.json"
            columns_file = selected_folder / "columns_data.json"
            
            # Отображаем статус файлов
            col_parquet, col_workflow, col_columns = st.columns(3)
            
            with col_parquet:
                if parquet_files:
                    st.success("✅ Parquet файл найден")
                    st.caption(f"📄 {parquet_files[0].name}")
                    input_path = str(parquet_files[0])
                else:
                    st.error("❌ Parquet файл не найден")
                    input_path = None
            
            with col_workflow:
                if workflow_file.exists():
                    st.success("✅ Workflow найден")
                    st.caption(f"⚙️ {workflow_file.name}")
                    workflow_path = str(workflow_file)
                else:
                    st.error("❌ Workflow файл не найден")
                    workflow_path = None
            
            with col_columns:
                if columns_file.exists():
                    st.success("✅ Columns data найден")
                    st.caption(f"📊 {columns_file.name}")
                    columns_data_path = str(columns_file)
                else:
                    st.error("❌ Columns data файл не найден")
                    columns_data_path = None
            
            # Проверяем, что все файлы найдены
            all_files_present = all([input_path, workflow_path, columns_data_path])
            
            if all_files_present:
                st.success("🎉 Все необходимые файлы найдены!")
            else:
                missing_files = []
                if not input_path:
                    missing_files.append("Parquet файл")
                if not workflow_path:
                    missing_files.append("Workflow файл")
                if not columns_data_path:
                    missing_files.append("Columns data файл")
                
                st.error(f"❌ Отсутствуют файлы: {', '.join(missing_files)}")
                st.info("Убедитесь, что в выбранной папке есть все необходимые файлы.")
        else:
            input_path = None
            workflow_path = None
            columns_data_path = None

with col2:
    st.subheader("⚙️ Настройки")
    
    # Параметры обработки
    chunk_size = st.number_input("Размер чанка:", value=100000, min_value=10000, max_value=1000000)
    max_memory = st.slider("Максимальная память (%):", value=80, min_value=50, max_value=95)
    
    # Формат экспорта
    export_format = st.selectbox("Формат экспорта:", ["parquet", "csv"])
    
    # Переопределение формата
    override_format = st.checkbox("Переопределить формат из workflow", value=False)
    
    # Имя выходного файла
    output_filename = st.text_input(
        "Имя выходного файла:",
        value=f"processed_{export_format}.{export_format}"
    )
    
    # Анализ кэш (автоматически из выбранной папки)
    if 'columns_data_path' in locals() and columns_data_path:
        st.success(f"✅ Columns data: {Path(columns_data_path).name}")
        analyze_cache_path = columns_data_path
    else:
        analyze_cache_path = st.text_input(
            "Путь к analyze_cache (опционально):",
            value="",
            help="Путь к файлу columns_data.json для ускорения анализа"
        )

# Валидация и предварительный просмотр
if st.button("🔍 Проверить workflow", type="secondary"):
    if workflow_path and Path(workflow_path).exists():
        try:
            with open(workflow_path, 'r', encoding='utf-8') as f:
                workflow_data = json.load(f)
            
            st.success("✅ Workflow файл валиден")
            
            # Показываем структуру workflow
            st.subheader("📋 Структура workflow:")
            st.json(workflow_data)
            
            # Показываем шаги обработки
            if 'steps' in workflow_data:
                st.subheader("🔄 Шаги обработки:")
                for i, step in enumerate(workflow_data['steps'], 1):
                    st.write(f"{i}. {step.get('name', 'Неизвестный шаг')}")
            
        except json.JSONDecodeError as e:
            st.error(f"❌ Ошибка в JSON файле: {e}")
        except Exception as e:
            st.error(f"❌ Ошибка чтения workflow: {e}")
    else:
        st.warning("⚠️ Укажите путь к workflow файлу")

# Кнопка запуска
if st.button("🚀 Запустить обработку", type="primary"):
    if not input_path or not workflow_path:
        st.error("❌ Выберите папку с готовыми файлами")
    else:
        # Проверяем существование файлов
        input_file = Path(input_path)
        workflow_file = Path(workflow_path)
        
        if not input_file.exists():
            st.error(f"❌ Входной файл не найден: {input_path}")
        elif not workflow_file.exists():
            st.error(f"❌ Workflow файл не найден: {workflow_path}")
        else:
            # Создаем выходной путь
            output_path = Path("exports") / output_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Формируем команду
            cmd = [
                'python', 'cli_process.py',
                '--path', str(input_file),
                '--workflow', str(workflow_file),
                '--output', str(output_path),
                '--chunk-size', str(chunk_size),
                '--max-memory', str(max_memory)
            ]
            
            # Добавляем опциональные параметры
            if analyze_cache_path and Path(analyze_cache_path).exists():
                cmd.extend(['--analyze_cache', str(analyze_cache_path)])
            
            if override_format:
                cmd.extend(['--format', export_format])
            
            st.info(f"🚀 Запуск команды: {' '.join(cmd)}")
            
            # Запускаем процесс
            try:
                # Создаем контейнеры для прогресса
                progress_container = st.container()
                status_container = st.container()
                
                with progress_container:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                
                with status_container:
                    log_container = st.empty()
                
                # Запускаем процесс с отслеживанием прогресса
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                # Отслеживаем прогресс
                stdout_lines = []
                stderr_lines = []
                
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        stdout_lines.append(output.strip())
                        # Обновляем логи в реальном времени
                        with log_container:
                            st.text_area("Логи обработки:", value="\n".join(stdout_lines[-20:]), height=200)
                
                # Получаем финальный результат
                stdout, stderr = process.communicate()
                result = subprocess.CompletedProcess(
                    args=cmd,
                    returncode=process.returncode,
                    stdout=stdout,
                    stderr=stderr
                )
                
                # Обновляем прогресс
                progress_bar.progress(1.0)
                status_text.text("✅ Обработка завершена!")
                
                # Показываем результат
                if result.returncode == 0:
                    st.success("✅ Обработка завершена успешно!")
                    
                    # Показываем информацию о результате
                    if output_path.exists():
                        file_size = output_path.stat().st_size
                        st.metric("Размер результата", f"{file_size / (1024*1024):.1f} MB")
                        st.info(f"📁 Результат сохранен: {output_path}")
                        
                        # Добавляем кнопку скачивания
                        with open(output_path, "rb") as file:
                            st.download_button(
                                label="📥 Скачать результат",
                                data=file.read(),
                                file_name=output_path.name,
                                mime="application/octet-stream"
                            )
                        
                        # Сохраняем в сессию для истории
                        if "processed_files" not in st.session_state:
                            st.session_state.processed_files = []
                        
                        st.session_state.processed_files.append({
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "input_file": str(input_file),
                            "output_file": str(output_path),
                            "file_size": file_size,
                            "chunk_size": chunk_size,
                            "max_memory": max_memory
                        })
                    
                    # Показываем логи
                    if result.stdout:
                        st.subheader("📋 Логи обработки:")
                        st.text_area("", value=result.stdout, height=300)
                
                else:
                    st.error("❌ Обработка завершилась с ошибкой")
                    st.subheader("📋 Логи ошибок:")
                    st.text_area("", value=result.stderr or result.stdout, height=300)
                    
            except subprocess.TimeoutExpired:
                st.error("⏰ Обработка превысила время ожидания (1 час)")
            except Exception as e:
                st.error(f"❌ Ошибка запуска: {e}")

# Информация о системе
with st.expander("ℹ️ Информация о системе"):
    try:
        import polars as pl
        st.write(f"**Polars версия:** {pl.__version__}")
    except:
        st.write("Polars не установлен")
    
    try:
        import psutil
        memory = psutil.virtual_memory()
        st.write(f"**Использование памяти:** {memory.percent:.1f}%")
        st.write(f"**Доступно памяти:** {memory.available / (1024**3):.1f} GB")
    except:
        st.write("Информация о памяти недоступна")

# Быстрые команды
st.subheader("🖥️ Быстрые команды")

# Предустановленные команды
if st.button("📋 Показать CLI команду"):
    if input_path and workflow_path:
        cli_command = f"""python cli_process.py \\
    --path {input_path} \\
    --workflow {workflow_path} \\
    --output {output_path} \\
    --chunk-size {chunk_size} \\
    --max-memory {max_memory}"""
        
        st.code(cli_command, language="bash")
    else:
        st.warning("Сначала укажите пути к файлам")

# Информация о файлах
if st.button("📊 Информация о файлах"):
    if input_path and Path(input_path).exists():
        try:
            import polars as pl
            if input_path.endswith('.parquet'):
                df = pl.scan_parquet(input_path)
            else:
                df = pl.scan_csv(input_path)
            
            row_count = df.select(pl.len()).collect().item()
            schema = df.collect_schema()
            
            st.success(f"✅ Файл найден: {input_path}")
            st.write(f"**Строк:** {row_count:,}")
            st.write(f"**Колонок:** {len(schema)}")
            st.write(f"**Размер:** {Path(input_path).stat().st_size / (1024*1024):.1f} MB")
            
        except Exception as e:
            st.error(f"❌ Ошибка анализа файла: {e}")
    else:
        st.warning("Файл не найден или путь не указан")

# История обработки
if "processed_files" in st.session_state and st.session_state.processed_files:
    st.subheader("📚 История обработки")
    
    for i, file_info in enumerate(reversed(st.session_state.processed_files[-5:])):  # Показываем последние 5
        with st.expander(f"🕒 {file_info['timestamp']} - {Path(file_info['input_file']).name}"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write(f"**Входной файл:** {Path(file_info['input_file']).name}")
                st.write(f"**Выходной файл:** {Path(file_info['output_file']).name}")
            
            with col2:
                st.write(f"**Размер результата:** {file_info['file_size'] / (1024*1024):.1f} MB")
                st.write(f"**Размер чанка:** {file_info['chunk_size']:,}")
            
            with col3:
                st.write(f"**Макс. память:** {file_info['max_memory']}%")
                
                # Кнопка для повторного скачивания
                if Path(file_info['output_file']).exists():
                    with open(file_info['output_file'], "rb") as file:
                        st.download_button(
                            label="📥 Скачать",
                            data=file.read(),
                            file_name=Path(file_info['output_file']).name,
                            mime="application/octet-stream",
                            key=f"download_{i}"
                        )
                else:
                    st.warning("Файл не найден")

# Очистка истории
if st.button("🗑️ Очистить историю"):
    if "processed_files" in st.session_state:
        del st.session_state.processed_files
        st.success("История очищена")
        st.rerun()

# Справка и документация
with st.expander("❓ Справка и документация"):
    st.markdown("""
    ### 🚀 Simple Data Processor
    
    **Описание:** Упрощенный интерфейс для CLI процессора данных с поддержкой потоковой обработки больших файлов.
    
    **Возможности:**
    - 📁 Выбор папки с готовыми файлами из exports
    - ⚙️ Настройка параметров обработки (размер чанка, память)
    - 🔍 Валидация workflow файлов
    - 📊 Анализ входных файлов
    - 🚀 Запуск обработки с мониторингом в реальном времени
    - 📥 Скачивание результатов
    - 📚 История обработки
    
    **Поддерживаемые форматы:**
    - Входные: Parquet, CSV
    - Workflow: JSON
    - Выходные: Parquet, CSV
    
    **Параметры обработки:**
    - **Размер чанка:** Количество строк для обработки за раз (10,000 - 1,000,000)
    - **Максимальная память:** Процент использования RAM (50% - 95%)
    - **Формат экспорта:** Переопределение формата из workflow
    
    **Структура папки exports:**
    ```
    exports/
    └── project_name_timestamp/
        ├── data.parquet          # Обработанные данные
        ├── workflow.json         # Настройки обработки  
        └── columns_data.json     # Метаданные колонок
    ```
    
    **Требования:**
    - Python 3.8+
    - Polars
    - Streamlit
    - psutil
    """)
    
    st.subheader("🔧 Примеры использования")
    
    st.code("""
# Запуск через CLI
python cli_process.py \\
    --path data.parquet \\
    --workflow workflow.json \\
    --output result.parquet \\
    --chunk-size 100000 \\
    --max-memory 80
    """, language="bash")
    
    st.subheader("📋 Структура workflow.json")
    st.code("""
{
    "name": "Data Cleaning Workflow",
    "steps": [
        {
            "name": "Load File",
            "type": "load_file",
            "params": {...}
        },
        {
            "name": "Remove Duplicates", 
            "type": "remove_duplicates",
            "params": {...}
        }
    ]
}
    """, language="json")



