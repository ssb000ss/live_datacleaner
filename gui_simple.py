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
# Убираем импорты для генерации имени файла - будем читать из workflow

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

    # Формат экспорта фиксирован: parquet
    export_format = "parquet"

    # Имя выходного файла читаем из workflow
    output_filename = "processed_parquet.parquet"  # fallback
    if workflow_path and workflow_path.exists():
        try:
            with open(workflow_path, 'r', encoding='utf-8') as f:
                workflow_data = json.load(f)
                if "output_filename" in workflow_data:
                    output_filename = workflow_data["output_filename"]
                    st.info(f"📄 Имя файла из workflow: `{output_filename}`")
                else:
                    st.warning("⚠️ В workflow не найдено имя выходного файла")
        except Exception as e:
            st.error(f"❌ Ошибка чтения workflow: {e}")
    else:
        st.warning("⚠️ Workflow файл не выбран")

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

            # Формат не переопределяем — всегда parquet

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
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )

                # Отслеживаем прогресс (храним только последние 200 строк для экономии памяти)
                stdout_lines = []

                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        stdout_lines.append(output.strip())
                        if len(stdout_lines) > 200:
                            stdout_lines = stdout_lines[-200:]
                        # Обновляем логи в реальном времени
                        with log_container:
                            st.text_area("Логи обработки:", value="\n".join(stdout_lines[-50:]), height=200)

                # Получаем финальный результат
                stdout, _ = process.communicate()
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
                        st.metric("Размер результата", f"{file_size / (1024 * 1024):.1f} MB")
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
                        st.subheader("📋 Логи обработки (последние 1000 символов):")
                        st.text_area("", value=result.stdout[-1000:], height=300)

                else:
                    st.error("❌ Обработка завершилась с ошибкой")
                    st.subheader("📋 Логи (последние 2000 символов):")
                    err_text = result.stdout or ""
                    st.text_area("", value=err_text[-2000:], height=300)

            except subprocess.TimeoutExpired:
                st.error("⏰ Обработка превысила время ожидания (1 час)")
            except Exception as e:
                st.error(f"❌ Ошибка запуска: {e}")

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
                st.write(f"**Размер результата:** {file_info['file_size'] / (1024 * 1024):.1f} MB")
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
