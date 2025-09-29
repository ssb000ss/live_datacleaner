"""
Оптимизированный потоковый процессор данных с использованием Polars.
Предназначен для обработки больших файлов без загрузки в память.
"""

import logging
import gc
import psutil
from pathlib import Path
from typing import Optional, Dict, Any, List, Iterator
import polars as pl
from datetime import datetime
import tempfile
import shutil

logger = logging.getLogger(__name__)


class StreamingDataProcessor:
    """Потоковый процессор данных с оптимизацией памяти"""
    
    def __init__(self, 
                 chunk_size: int = 100000,
                 max_memory_percent: float = 75.0,
                 temp_dir: Optional[Path] = None):
        self.chunk_size = chunk_size
        self.max_memory_percent = max_memory_percent
        self.temp_dir = temp_dir or Path(tempfile.gettempdir()) / "streaming_processor"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
    def check_memory_usage(self) -> bool:
        """Проверяет использование памяти и при необходимости очищает кеш"""
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > self.max_memory_percent:
            logger.warning(f"High memory usage: {memory_percent:.1f}%, forcing garbage collection")
            gc.collect()
            memory_percent = psutil.virtual_memory().percent
        return memory_percent <= self.max_memory_percent
    
    def get_memory_info(self) -> Dict[str, float]:
        """Возвращает информацию об использовании памяти"""
        memory = psutil.virtual_memory()
        return {
            'total_gb': memory.total / (1024**3),
            'available_gb': memory.available / (1024**3),
            'used_gb': memory.used / (1024**3),
            'percent': memory.percent
        }
    
    def scan_file_streaming(self, file_path: Path, **kwargs) -> pl.LazyFrame:
        """Сканирует файл в потоковом режиме"""
        try:
            if file_path.suffix.lower() == '.parquet':
                return pl.scan_parquet(str(file_path), **kwargs)
            else:
                # Настройки по умолчанию для CSV
                default_kwargs = {
                    'encoding': 'utf8',
                    'ignore_errors': True,
                    'infer_schema_length': 1000,
                    'rechunk': False,
                    'truncate_ragged_lines': True
                }
                default_kwargs.update(kwargs)
                return pl.scan_csv(str(file_path), **default_kwargs)
        except Exception as e:
            logger.error(f"Ошибка сканирования файла {file_path}: {e}")
            raise
    
    def process_in_streaming_chunks(self, 
                                  ldf: pl.LazyFrame, 
                                  operation_func,
                                  *args, 
                                  **kwargs) -> pl.LazyFrame:
        """Обрабатывает данные по частям в потоковом режиме"""
        try:
            # Получаем общее количество строк
            total_rows = ldf.select(pl.len()).collect().item()
            logger.info(f"Обработка {total_rows:,} строк в потоковом режиме")
            
            if total_rows <= self.chunk_size:
                # Если данных мало, обрабатываем целиком
                return operation_func(ldf, *args, **kwargs)
            
            # Обрабатываем по частям
            processed_chunks = []
            temp_files = []
            
            try:
                for offset in range(0, total_rows, self.chunk_size):
                    if not self.check_memory_usage():
                        logger.error("Недостаточно памяти для продолжения обработки")
                        return None
                    
                    # Получаем чанк данных
                    chunk = ldf.slice(offset, self.chunk_size)
                    processed_chunk = operation_func(chunk, *args, **kwargs)
                    
                    if processed_chunk is not None:
                        # Сохраняем чанк во временный файл
                        temp_file = self.temp_dir / f"chunk_{offset}.parquet"
                        processed_chunk.write_parquet(
                            str(temp_file),
                            compression="zstd",
                            compression_level=1
                        )
                        temp_files.append(temp_file)
                    
                    # Очищаем память
                    del processed_chunk
                    gc.collect()
                    
                    progress = (offset + self.chunk_size) / total_rows * 100
                    logger.info(f"Обработано: {progress:.1f}% ({offset + self.chunk_size:,}/{total_rows:,})")
                
                if not temp_files:
                    return None
                
                # Объединяем результаты из временных файлов
                logger.info("Объединение результатов...")
                result_lazy = pl.scan_parquet(str(temp_files[0]))
                for temp_file in temp_files[1:]:
                    chunk_lazy = pl.scan_parquet(str(temp_file))
                    result_lazy = result_lazy.vstack(chunk_lazy)
                
                return result_lazy
                
            finally:
                # Очищаем временные файлы
                for temp_file in temp_files:
                    try:
                        temp_file.unlink()
                    except Exception as e:
                        logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка потоковой обработки: {e}")
            return None
    
    def apply_column_operations_streaming(self, 
                                        ldf: pl.LazyFrame, 
                                        workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Применяет операции с колонками в потоковом режиме"""
        try:
            # Получаем схему для проверки существования колонок
            schema = ldf.collect_schema()
            available_columns = set(schema.names())
            
            # Исключаем ненужные колонки
            exclude_columns = workflow.get("columns", {}).get("exclude", [])
            if exclude_columns:
                # Фильтруем только существующие колонки
                valid_exclude = [col for col in exclude_columns if col in available_columns]
                if valid_exclude:
                    ldf = ldf.drop(valid_exclude)
                    logger.info(f"Исключены колонки: {valid_exclude}")
                if len(valid_exclude) != len(exclude_columns):
                    missing = set(exclude_columns) - set(valid_exclude)
                    logger.warning(f"Колонки для исключения не найдены: {missing}")
            
            # Применяем конкатенации (до исключения колонок)
            concatenations = workflow.get("concatenations", [])
            for concat in concatenations:
                source_cols = concat["source_columns"]
                separator = concat["separator"]
                new_col = concat["name"]
                
                # Проверяем, что все исходные колонки существуют
                valid_source_cols = [col for col in source_cols if col in available_columns]
                if valid_source_cols and len(valid_source_cols) == len(source_cols):
                    ldf = ldf.with_columns([
                        pl.concat_str([pl.col(col) for col in source_cols], separator=separator).alias(new_col)
                    ])
                    logger.info(f"Создана конкатенация: {new_col} из {source_cols}")
                else:
                    missing = set(source_cols) - set(valid_source_cols)
                    logger.warning(f"Пропущена конкатенация {new_col}: отсутствуют колонки {missing}")
            
            # Получаем обновленную схему после исключения колонок и конкатенаций
            updated_schema = ldf.collect_schema()
            updated_columns = set(updated_schema.names())
            
            # Применяем regex правила только к существующим колонкам
            regex_rules = workflow.get("regex_rules", {})
            for col, patterns in regex_rules.items():
                if patterns and col in updated_columns:
                    # Комбинируем паттерны
                    combined_pattern = "|".join(patterns)
                    ldf = ldf.with_columns([
                        pl.col(col)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.extract_all(combined_pattern)
                        .list.eval(pl.element().filter(pl.element().str.len_chars() > 0), parallel=True)
                        .list.join("")
                        .fill_null("")
                        .alias(col)
                    ])
                    logger.info(f"Применены regex правила для колонки: {col}")
                elif patterns:
                    logger.warning(f"Пропущены regex правила для колонки {col}: колонка не найдена")
            
            # Применяем переименование колонок
            display_names = workflow.get("display_names", {})
            if display_names:
                # Фильтруем только существующие колонки
                valid_renames = {old_name: new_name for old_name, new_name in display_names.items() 
                               if old_name in updated_columns and old_name != new_name}
                if valid_renames:
                    ldf = ldf.rename(valid_renames)
                    logger.info(f"Переименованы колонки: {valid_renames}")
                    
                    # Обновляем имена колонок в настройках workflow для последующих операций
                    self._update_workflow_column_names_streaming(workflow, valid_renames)
            
            # Применяем очистку данных
            ldf = self._apply_data_cleaning_streaming(ldf)
            
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка применения операций с колонками: {e}")
            raise
    
    def _update_workflow_column_names_streaming(self, workflow: Dict[str, Any], renames: Dict[str, str]):
        """Обновляет имена колонок в настройках workflow после переименования"""
        try:
            # Обновляем имена в настройках дедупликации
            if "dedup" in workflow and "unique_columns" in workflow["dedup"]:
                old_columns = workflow["dedup"]["unique_columns"]
                new_columns = [renames.get(col, col) for col in old_columns]
                workflow["dedup"]["unique_columns"] = new_columns
                logger.info(f"Обновлены имена колонок в дедупликации: {old_columns} -> {new_columns}")
            
            # Обновляем имена в настройках валидации
            if "not_empty" in workflow and "columns" in workflow["not_empty"]:
                old_columns = workflow["not_empty"]["columns"]
                new_columns = [renames.get(col, col) for col in old_columns]
                workflow["not_empty"]["columns"] = new_columns
                logger.info(f"Обновлены имена колонок в валидации: {old_columns} -> {new_columns}")
                
        except Exception as e:
            logger.error(f"Ошибка обновления имен колонок в workflow: {e}")
    
    def _apply_data_cleaning_streaming(self, ldf: pl.LazyFrame) -> pl.LazyFrame:
        """Применяет базовую очистку данных в потоковом режиме"""
        try:
            # Получаем схему для определения строковых колонок
            schema = ldf.collect_schema()
            string_cols = [col for col, dtype in schema.items() if dtype == pl.Utf8]
            
            if string_cols:
                # Очистка от кавычек и специальных символов
                ldf = ldf.with_columns([
                    pl.col(col)
                    .str.replace_all(r'["\']', '')
                    .str.replace_all(r'&nbsp;|\\n|\\t|\\r|\xa0|\ufeff', ' ')
                    .str.replace_all(r'[\n\r\t]', ' ')
                    .str.replace_all(r'[\u00a0\u202f\u2007\u1680\u180e\u205f]', ' ')
                    .str.replace_all(r'[\u200b\u200c\u200d\u2060\u00ad\u200e\u200f\u061c]', '')
                    .str.replace_all(r'[\x00-\x1F\x7F]', '')
                    .str.replace_all(r'\s{2,}', ' ')
                    .str.strip_chars()
                    .alias(col)
                    for col in string_cols
                ])
                
                # Замена пустых значений
                ldf = ldf.with_columns([
                    pl.when(
                        pl.col(col).str.strip_chars().is_in(["", " ", "nan", "NaN", "none", "None", "null", "NULL", "0"]) |
                        pl.col(col).is_null()
                    )
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                    for col in string_cols
                ])
            
            logger.info("Применена очистка данных в потоковом режиме")
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка очистки данных: {e}")
            raise
    
    def apply_deduplication_streaming(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Применяет дедупликацию в потоковом режиме"""
        try:
            dedup_settings = workflow.get("dedup", {})
            unique_columns = dedup_settings.get("unique_columns", [])
            
            # Получаем все колонки из схемы
            schema_cols = list(ldf.collect_schema().names())
            
            # Если unique_columns не указаны или пустые, используем все колонки по умолчанию
            if not unique_columns:
                unique_columns = schema_cols
                logger.info("Дедупликация по умолчанию: все колонки")
            else:
                # Проверяем, что указанные колонки существуют
                valid_columns = [col for col in unique_columns if col in schema_cols]
                
                if valid_columns:
                    unique_columns = valid_columns
                    logger.info(f"Дедупликация по указанным колонкам: {unique_columns}")
                else:
                    logger.warning(f"Колонки для дедупликации не найдены: {unique_columns}, используем все колонки")
                    unique_columns = schema_cols
            
            # Применяем дедупликацию
            ldf = ldf.unique(subset=unique_columns)
            logger.info(f"Применена дедупликация по колонкам: {unique_columns}")
            
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка дедупликации: {e}")
            raise
    
    def apply_validation_streaming(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Применяет валидацию данных в потоковом режиме"""
        try:
            not_empty_settings = workflow.get("not_empty", {})
            required_columns = not_empty_settings.get("columns", [])
            
            if required_columns:
                # Проверяем, что колонки существуют
                schema_cols = set(ldf.collect_schema().names())
                valid_columns = [col for col in required_columns if col in schema_cols]
                
                if valid_columns:
                    ldf = ldf.drop_nulls(subset=valid_columns)
                    logger.info(f"Удалены строки с пустыми значениями в колонках: {valid_columns}")
                else:
                    logger.warning(f"Колонки для валидации не найдены: {required_columns}")
            
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка валидации: {e}")
            raise
    
    def process_file_streaming(self, 
                             input_path: Path, 
                             workflow: Dict[str, Any], 
                             output_path: Path) -> bool:
        """Обрабатывает файл в потоковом режиме"""
        try:
            logger.info(f"Начинаем потоковую обработку: {input_path}")
            start_time = datetime.now()
            
            # Загружаем данные лениво
            ldf = self.scan_file_streaming(input_path)
            
            # Получаем общее количество строк
            total_rows = ldf.select(pl.len()).collect().item()
            logger.info(f"Всего строк для обработки: {total_rows:,}")
            
            # Применяем операции из workflow
            ldf = self.apply_column_operations_streaming(ldf, workflow)
            ldf = self.apply_deduplication_streaming(ldf, workflow)
            ldf = self.apply_validation_streaming(ldf, workflow)
            
            # Выполняем в потоковом режиме
            logger.info("Запуск потоковой обработки...")
            result_df = ldf.collect(engine="streaming")
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            logger.info(f"Обработка завершена за {processing_time:.2f} секунд")
            logger.info(f"Обработано строк: {len(result_df):,}")
            
            # Сохраняем результат
            self._save_result_streaming(result_df, output_path, workflow)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка потоковой обработки: {e}")
            return False
    
    def _save_result_streaming(self, df: pl.DataFrame, output_path: Path, workflow: Dict[str, Any]):
        """Сохраняет результат обработки с оптимизацией"""
        try:
            export_settings = workflow.get("export", {})
            format_type = export_settings.get("format", "parquet")
            
            # Определяем формат по расширению файла, если не указан в workflow
            if output_path.suffix.lower() == '.csv':
                format_type = "csv"
            elif output_path.suffix.lower() == '.parquet':
                format_type = "parquet"
            
            # Создаем директорию для выходного файла
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if format_type == "parquet":
                parquet_settings = export_settings.get("parquet", {})
                compression = parquet_settings.get("compression", "zstd")
                
                # Убеждаемся, что расширение правильное
                if not output_path.suffix.lower() == '.parquet':
                    output_path = output_path.with_suffix('.parquet')
                
                # Сохраняем с оптимизацией
                df.write_parquet(
                    str(output_path),
                    compression=compression,
                    compression_level=3,
                    maintain_order=False
                )
                
            elif format_type == "csv":
                csv_settings = export_settings.get("csv", {})
                delimiter = csv_settings.get("delimiter", "~")
                quote_all = csv_settings.get("quote_all", True)
                
                # Убеждаемся, что расширение правильное
                if not output_path.suffix.lower() == '.csv':
                    output_path = output_path.with_suffix('.csv')
                
                # Polars write_csv параметры
                df.write_csv(
                    str(output_path),
                    separator=delimiter
                )
            
            logger.info(f"Результат сохранен: {output_path}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения результата: {e}")
            raise
    
    def cleanup_temp_files(self):
        """Очищает временные файлы"""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                logger.info("Временные файлы очищены")
        except Exception as e:
            logger.warning(f"Ошибка очистки временных файлов: {e}")


class MemoryOptimizedDataLoader:
    """Оптимизированный загрузчик данных с контролем памяти"""
    
    def __init__(self, 
                 parquet_folder: Path,
                 chunk_size: int = 100000,
                 max_memory_percent: float = 75.0):
        self.parquet_folder = parquet_folder
        self.parquet_folder.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self.max_memory_percent = max_memory_percent
        self.processor = StreamingDataProcessor(chunk_size, max_memory_percent)
    
    def load_csv_streaming(self, 
                          path: Path, 
                          encoding: str = "utf8", 
                          delimiter: str = ",") -> Optional[pl.LazyFrame]:
        """Загружает CSV файл в потоковом режиме"""
        if not path.is_file():
            logger.error(f"Файл не найден: {path}")
            return None
        
        if not self.processor.check_memory_usage():
            memory_info = self.processor.get_memory_info()
            logger.error(f"Недостаточно памяти для загрузки файла. Использование: {memory_info['percent']:.1f}%")
            return None
        
        try:
            # Используем потоковое сканирование
            lazy_df = self.processor.scan_file_streaming(
                path,
                encoding=encoding,
                separator=delimiter,
                ignore_errors=True,
                null_values=["", " ", "\t", "NULL", "null", "NaN", "nan", "None", "none"],
                truncate_ragged_lines=True,
                quote_char=None,
                infer_schema_length=1000,
                rechunk=False
            )
            
            # Очищаем названия колонок
            schema = lazy_df.collect_schema()
            original_names = list(schema.names())
            cleaned_names = [name.replace('"', '').strip() for name in original_names]
            
            if cleaned_names != original_names:
                rename_map = {orig: new for orig, new in zip(original_names, cleaned_names) if orig != new}
                if rename_map:
                    lazy_df = lazy_df.rename(rename_map)
            
            logger.info(f"CSV файл загружен в потоковом режиме: {path}")
            return lazy_df
            
        except Exception as e:
            logger.error(f"Ошибка загрузки CSV файла {path}: {e}")
            return None
    
    def save_parquet_streaming(self, lazy_df: pl.LazyFrame, parquet_path: Path) -> bool:
        """Сохраняет LazyFrame в Parquet в потоковом режиме"""
        try:
            # Используем sink_parquet для потокового сохранения
            lazy_df.sink_parquet(
                str(parquet_path),
                compression="zstd",
                compression_level=3,
                maintain_order=False
            )
            logger.info(f"Parquet файл сохранен в потоковом режиме: {parquet_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения Parquet файла {parquet_path}: {e}")
            return False
    
    def get_parquet_path(self, source_path: Path) -> Path:
        """Возвращает путь к Parquet файлу"""
        return self.parquet_folder / f"{source_path.stem}.parquet"
