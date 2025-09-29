#!/usr/bin/env python3
"""
CLI процессор для выполнения workflow данных в потоковом режиме.
Оптимизирован для работы с большими файлами без загрузки в память.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import polars as pl
import psutil
import gc
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cli_process.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StreamingDataProcessor:
    """Потоковый процессор данных с оптимизацией памяти"""
    
    def __init__(self, chunk_size: int = 50000, max_memory_percent: float = 80.0):
        self.chunk_size = chunk_size
        self.max_memory_percent = max_memory_percent
        self.processed_rows = 0
        self.total_rows = 0
        
    def check_memory_usage(self) -> bool:
        """Проверяет использование памяти"""
        memory_percent = psutil.virtual_memory().percent
        if memory_percent > self.max_memory_percent:
            logger.warning(f"High memory usage: {memory_percent:.1f}%, forcing garbage collection")
            gc.collect()
            memory_percent = psutil.virtual_memory().percent
        return memory_percent <= self.max_memory_percent
    
    def load_workflow(self, workflow_path: Path) -> Dict[str, Any]:
        """Загружает workflow из JSON файла"""
        try:
            with open(workflow_path, 'r', encoding='utf-8') as f:
                workflow = json.load(f)
            logger.info(f"Workflow загружен: {workflow_path}")
            return workflow
        except Exception as e:
            logger.error(f"Ошибка загрузки workflow: {e}")
            raise
    
    def apply_column_operations(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Применяет операции с колонками из workflow"""
        try:
            # Получаем схему для проверки существования колонок
            schema = ldf.collect_schema()
            available_columns = set(schema.names())
            
            # 1. Сначала применяем regex правила к исходным колонкам
            from utils.config import REGEX_PATTERNS_UNICODE
            regex_rules = workflow.get("regex_rules", {})
            for col, patterns in regex_rules.items():
                if patterns and col in available_columns:
                    # Комбинируем паттерны из REGEX_PATTERNS_UNICODE
                    pattern_strings = [REGEX_PATTERNS_UNICODE.get(p, p) for p in patterns if p in REGEX_PATTERNS_UNICODE]
                    combined_pattern = "|".join(pattern_strings)
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
            
            # 2. Применяем конкатенации (после regex обработки исходных колонок)
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
            
            # 3. Исключаем ненужные колонки (после создания конкатенаций)
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
            
            # 4. Применяем regex правила к новым колонкам (например, fio)
            updated_schema = ldf.collect_schema()
            updated_columns = set(updated_schema.names())
            
            for col, patterns in regex_rules.items():
                if patterns and col in updated_columns and col not in available_columns:
                    # Это новая колонка (например, fio), применяем regex правила
                    pattern_strings = [REGEX_PATTERNS_UNICODE.get(p, p) for p in patterns if p in REGEX_PATTERNS_UNICODE]
                    combined_pattern = "|".join(pattern_strings)
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
                    logger.info(f"Применены regex правила для новой колонки: {col}")
            
            # 5. Применяем переименование колонок
            display_names = workflow.get("display_names", {})
            if display_names:
                # Фильтруем только существующие колонки
                valid_renames = {old_name: new_name for old_name, new_name in display_names.items() 
                               if old_name in updated_columns and old_name != new_name}
                if valid_renames:
                    ldf = ldf.rename(valid_renames)
                    logger.info(f"Переименованы колонки: {valid_renames}")
                    
                    # Обновляем имена колонок в настройках workflow для последующих операций
                    self._update_workflow_column_names(workflow, valid_renames)
            
            # 6. Применяем очистку данных
            ldf = self._apply_data_cleaning(ldf)
            
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка применения операций с колонками: {e}")
            raise
    
    def _update_workflow_column_names(self, workflow: Dict[str, Any], renames: Dict[str, str]):
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
    
    def _apply_data_cleaning(self, ldf: pl.LazyFrame) -> pl.LazyFrame:
        """Применяет базовую очистку данных"""
        try:
            # Очистка специальных символов
            string_cols = [col for col, dtype in ldf.collect_schema().items() if dtype == pl.Utf8]
            
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
            
            logger.info("Применена очистка данных")
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка очистки данных: {e}")
            raise
    
    def apply_deduplication(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Применяет дедупликацию"""
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
    
    def apply_validation(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Применяет валидацию данных"""
        try:
            not_empty_settings = workflow.get("not_empty", {})
            required_columns = not_empty_settings.get("columns", [])
            
            if required_columns:
                ldf = ldf.drop_nulls(subset=required_columns)
                logger.info(f"Удалены строки с пустыми значениями в колонках: {required_columns}")
            
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка валидации: {e}")
            raise
    
    def process_streaming(self, input_path: Path, workflow: Dict[str, Any], output_path: Path) -> bool:
        """Обрабатывает данные в потоковом режиме"""
        try:
            logger.info(f"Начинаем потоковую обработку: {input_path}")
            
            # Загружаем данные лениво
            if input_path.suffix.lower() == '.parquet':
                ldf = pl.scan_parquet(str(input_path))
            else:
                ldf = pl.scan_csv(str(input_path), encoding='utf8', ignore_errors=True)
            
            # Получаем общее количество строк
            self.total_rows = ldf.select(pl.len()).collect().item()
            logger.info(f"Всего строк для обработки: {self.total_rows:,}")
            
            # Применяем операции из workflow
            ldf = self.apply_column_operations(ldf, workflow)
            ldf = self.apply_deduplication(ldf, workflow)
            ldf = self.apply_validation(ldf, workflow)
            
            # Выполняем в потоковом режиме
            logger.info("Запуск потоковой обработки...")
            start_time = datetime.now()
            
            # Используем streaming engine для обработки больших файлов
            result_df = ldf.collect(engine="streaming")
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            logger.info(f"Обработка завершена за {processing_time:.2f} секунд")
            logger.info(f"Обработано строк: {len(result_df):,}")
            
            # Сохраняем результат
            self._save_result(result_df, output_path, workflow)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка потоковой обработки: {e}")
            return False
    
    def _save_result(self, df: pl.DataFrame, output_path: Path, workflow: Dict[str, Any]):
        """Сохраняет результат обработки"""
        try:
            export_settings = workflow.get("export", {})
            format_type = export_settings.get("format", "parquet")
            
            # Определяем формат по расширению файла, если не указан в workflow
            if output_path.suffix.lower() == '.csv':
                format_type = "csv"
            elif output_path.suffix.lower() == '.parquet':
                format_type = "parquet"
            
            if format_type == "parquet":
                parquet_settings = export_settings.get("parquet", {})
                compression = parquet_settings.get("compression", "zstd")
                target_mb = parquet_settings.get("target_mb_per_file", 100)
                
                # Убеждаемся, что расширение правильное
                if not output_path.suffix.lower() == '.parquet':
                    output_path = output_path.with_suffix('.parquet')
                
                # Сохраняем с оптимизацией
                df.write_parquet(
                    str(output_path),
                    compression=compression,
                    compression_level=3
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


def main():
    parser = argparse.ArgumentParser(description='CLI процессор для выполнения workflow данных')
    parser.add_argument('--path', required=True, help='Путь к входному файлу (parquet или csv)')
    parser.add_argument('--workflow', required=True, help='Путь к файлу workflow.json')
    parser.add_argument('--analyze_cache', help='Путь к файлу columns_data.json (опционально)')
    parser.add_argument('--output', help='Путь для сохранения результата (по умолчанию: input_processed.parquet)')
    parser.add_argument('--format', type=str, choices=['parquet', 'csv'], help='Формат выходного файла (переопределяет настройки workflow)')
    parser.add_argument('--chunk-size', type=int, default=50000, help='Размер чанка для обработки (по умолчанию: 50000)')
    parser.add_argument('--max-memory', type=float, default=80.0, help='Максимальное использование памяти в процентах (по умолчанию: 80)')
    
    args = parser.parse_args()
    
    # Валидация входных файлов
    input_path = Path(args.path)
    workflow_path = Path(args.workflow)
    
    if not input_path.exists():
        logger.error(f"Входной файл не найден: {input_path}")
        sys.exit(1)
    
    if not workflow_path.exists():
        logger.error(f"Файл workflow не найден: {workflow_path}")
        sys.exit(1)
    
    # Определяем выходной путь
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.parent / f"{input_path.stem}_processed.parquet"
    
    # Создаем процессор
    processor = StreamingDataProcessor(
        chunk_size=args.chunk_size,
        max_memory_percent=args.max_memory
    )
    
    try:
        # Загружаем workflow
        workflow = processor.load_workflow(workflow_path)
        
        # Переопределяем формат экспорта если указан
        if args.format:
            if "export" not in workflow:
                workflow["export"] = {}
            workflow["export"]["format"] = args.format
        
        # Обрабатываем данные
        success = processor.process_streaming(input_path, workflow, output_path)
        
        if success:
            logger.info("✅ Обработка завершена успешно!")
            print(f"Результат сохранен: {output_path}")
        else:
            logger.error("❌ Обработка завершилась с ошибкой")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
