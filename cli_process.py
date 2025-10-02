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
from utils.data_cleaner import lowercase_columns
from utils.filename_utils import generate_nomad_filename, get_current_year

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
            
            # 1. Конфигурация regex и конкатенаций
            from utils.config import REGEX_PATTERNS_UNICODE
            regex_rules = workflow.get("regex_rules", {})
            concatenations = workflow.get("concatenations", [])

            # Определяем целевые колонки с regex и источники, которые нужно предочистить regex целевой колонки
            targets_with_regex: set[str] = set()
            sources_preclean_by_target_regex: set[str] = set()
            for concat in concatenations:
                new_col = concat.get("name")
                if new_col and regex_rules.get(new_col):
                    targets_with_regex.add(new_col)
                    for src in concat.get("source_columns", []):
                        if src in available_columns:
                            sources_preclean_by_target_regex.add(src)

            # Предочистка источников regex целевой колонки (например, regex fio -> применяем к first_name/last_name)
            for concat in concatenations:
                new_col = concat.get("name")
                if new_col not in targets_with_regex:
                    continue
                patterns = regex_rules.get(new_col) or []
                pattern_strings = [REGEX_PATTERNS_UNICODE.get(p, p) for p in patterns if p in REGEX_PATTERNS_UNICODE]
                if not pattern_strings:
                    continue
                combined_pattern = "|".join(pattern_strings)
                for src in concat.get("source_columns", []):
                    if src in sources_preclean_by_target_regex:
                        ldf = ldf.with_columns([
                            pl.col(src)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .str.extract_all(combined_pattern)
                            .list.eval(pl.element().filter(pl.element().str.len_chars() > 0), parallel=True)
                            .list.join("")
                            .fill_null("")
                            .alias(src)
                        ])
                        logger.info(f"Предочистка regex (целевой {new_col}) применена к источнику: {src}")

            # 2. Конкатенации без последующего regex на целевой колонке
            for concat in concatenations:
                source_cols = concat["source_columns"]
                separator = concat["separator"]
                new_col = concat["name"]

                # Проверяем, что все исходные колонки существуют
                valid_source_cols = [col for col in source_cols if col in available_columns]
                if valid_source_cols and len(valid_source_cols) == len(source_cols):
                    expr = pl.concat_str([pl.col(col) for col in source_cols], separator=separator)
                    ldf = ldf.with_columns([expr.alias(new_col)])
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

            # 4. Применяем regex правила к исходным колонкам, КРОМЕ тех, что участвуют
            # в конкатенациях с целевым regex (мы их уже обработали через целевую колонку)
            updated_schema = ldf.collect_schema()
            updated_columns = set(updated_schema.names())

            for col, patterns in regex_rules.items():
                if not patterns:
                    continue
                # Цели с regex не трогаем (их regex уже применён к источникам)
                if col in targets_with_regex:
                    continue
                # Источники, к которым уже применяли регексы целевых, пропускаем
                if col in sources_preclean_by_target_regex:
                    continue
                if col in updated_columns:
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
                else:
                    logger.warning(f"Пропущены regex правила для колонки {col}: колонка не найдена")
            
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
            
            # 6. Автоматически переводим все строковые колонки в нижний регистр
            string_cols = [col for col, dtype in ldf.collect_schema().items() if dtype == pl.Utf8]
            if string_cols:
                ldf = lowercase_columns(ldf, string_cols)
                logger.info(f"Применен автоматический перевод в нижний регистр для всех строковых колонок: {string_cols}")
            
            # 7. Применяем очистку данных
            ldf = self._apply_data_cleaning(ldf)
            
            # 8. Применяем сборку вложенной структуры additional_info при наличии настроек
            ldf = self._apply_nesting(ldf, workflow)
            
            # 9. Добавляем год и код страны в финальную структуру
            ldf = self._add_metadata_fields(ldf, workflow)
            
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка применения операций с колонками: {e}")
            raise

    def _apply_nesting(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Собирает вложенную структуру additional_info и оставляет main_info на верхнем уровне.
        Ожидает структуру:
        workflow["structure"] = {"main_info": [...], "additional_info": ["col", {"group": ["a","b"]}, ...]}
        """
        try:
            structure = workflow.get("structure") or {}
            main_info = structure.get("main_info", [])
            additional_info = structure.get("additional_info", [])

            if not main_info and not additional_info:
                return ldf

            schema_names = set(ldf.collect_schema().names())

            # Собираем состав additional_info
            flat_fields: list[str] = []
            nested_structs: list[pl.Expr] = []

            group_keys = set()
            for item in additional_info:
                if isinstance(item, dict) and item:
                    group_keys.update(item.keys())

            for item in additional_info:
                if isinstance(item, str):
                    if item in schema_names and item not in group_keys:
                        flat_fields.append(item)
                elif isinstance(item, dict):
                    for nested_key, nested_fields in item.items():
                        valid_nested = [c for c in nested_fields if c in schema_names]
                        if valid_nested:
                            nested_structs.append(
                                pl.struct([pl.col(c) for c in valid_nested]).alias(nested_key)
                            )

            all_struct_fields: list[pl.Expr] = [pl.col(c) for c in flat_fields] + nested_structs
            if all_struct_fields:
                additional_struct = pl.struct(all_struct_fields).alias("additional_info")
            else:
                additional_struct = pl.lit(None).alias("additional_info")

            safe_main = [c for c in main_info if c != "additional_info" and c in schema_names]

            return ldf.select([pl.col(c) for c in safe_main] + [additional_struct])
        except Exception as e:
            logger.error(f"Ошибка сборки вложенной структуры: {e}")
            return ldf
    
    def _add_metadata_fields(self, ldf: pl.LazyFrame, workflow: Dict[str, Any]) -> pl.LazyFrame:
        """Добавляет год и код страны в финальную структуру данных"""
        try:
            # Получаем год и код страны из workflow
            year = workflow.get("year", get_current_year())
            country_code = workflow.get("country_code", "ru")
            
            # Добавляем поля как константы
            ldf = ldf.with_columns([
                pl.lit(year).alias("year"),
                pl.lit(country_code).alias("country_code")
            ])
            
            logger.info(f"Добавлены метаданные: year={year}, country_code={country_code}")
            return ldf
            
        except Exception as e:
            logger.error(f"Ошибка добавления метаданных: {e}")
            return ldf
    
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

            # Обновляем имена в структуре вывода (structure -> main_info, additional_info)
            structure = workflow.get("structure") or {}
            if structure:
                # main_info: список строк, возможно содержит 'additional_info' как зарезервированное поле
                main_info = structure.get("main_info", [])
                if isinstance(main_info, list):
                    updated_main = []
                    for name in main_info:
                        if name == "additional_info":
                            updated_main.append(name)
                        else:
                            updated_main.append(renames.get(name, name))
                    structure["main_info"] = updated_main

                # additional_info: список строк и/или dict {group: [fields]}
                additional_info = structure.get("additional_info", [])
                if isinstance(additional_info, list):
                    new_additional: list = []
                    for item in additional_info:
                        if isinstance(item, str):
                            new_additional.append(renames.get(item, item))
                        elif isinstance(item, dict):
                            new_item = {}
                            for grp, fields in item.items():
                                if isinstance(fields, list):
                                    new_fields = [renames.get(f, f) for f in fields]
                                    new_item[grp] = new_fields
                                else:
                                    new_item[grp] = fields
                            new_additional.append(new_item)
                        else:
                            new_additional.append(item)
                    structure["additional_info"] = new_additional

                workflow["structure"] = structure
                logger.info("Обновлены имена колонок в structure (main_info/additional_info)")
            
                
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
            
            # Пропускаем предварительный подсчет строк, чтобы избежать лишнего прохода по данным
            
            # Применяем операции из workflow
            ldf = self.apply_column_operations(ldf, workflow)
            ldf = self.apply_validation(ldf, workflow)
            ldf = self.apply_deduplication(ldf, workflow)
            
            # Выполняем в потоковом режиме, напрямую записывая результат без материализации в память
            logger.info("Запуск потоковой обработки и запись в Parquet...")
            start_time = datetime.now()

            # Убедимся, что расширение .parquet
            if output_path.suffix.lower() != '.parquet':
                output_path = output_path.with_suffix('.parquet')

            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Потоковая запись результата
            ldf.sink_parquet(
                str(output_path),
                compression="zstd",
                compression_level=3,
                maintain_order=False
            )

            # Пропускаем финальный подсчет строк, чтобы не выполнять второй проход по данным

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            logger.info(f"Обработка завершена за {processing_time:.2f} секунд")
            logger.info(f"Результат сохранен: {output_path}")

            return True
            
        except Exception as e:
            logger.error(f"Ошибка потоковой обработки: {e}")
            return False
    
    def _save_result(self, df: pl.DataFrame, output_path: Path, workflow: Dict[str, Any]):
        """Сохраняет результат обработки только в Parquet с дефолтным сжатием."""
        try:
            # Всегда сохраняем как Parquet независимо от настроек workflow
            if output_path.suffix.lower() != '.parquet':
                output_path = output_path.with_suffix('.parquet')

            df.write_parquet(
                str(output_path),
                compression="zstd",
                compression_level=3
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
    parser.add_argument('--output', help='Путь для сохранения результата (по умолчанию: из workflow)')
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
    
    # Создаем процессор
    processor = StreamingDataProcessor(
        chunk_size=args.chunk_size,
        max_memory_percent=args.max_memory
    )
    
    # Загружаем workflow для получения настроек
    workflow = processor.load_workflow(workflow_path)
    
    # Определяем выходной путь (всегда .parquet)
    if args.output:
        output_path = Path(args.output)
    else:
        # Используем имя файла из workflow, если есть
        if "output_filename" in workflow:
            output_path = input_path.parent / workflow["output_filename"]
            logger.info(f"Используется имя файла из workflow: {workflow['output_filename']}")
        else:
            # Fallback к простому имени
            output_path = input_path.parent / f"{input_path.stem}_processed.parquet"
            logger.info(f"Используется fallback имя файла: {output_path.name}")
    
    if output_path.suffix.lower() != '.parquet':
        output_path = output_path.with_suffix('.parquet')
    
    try:
        # Добавляем год и код страны в workflow (если не заданы в workflow)
        if "year" not in workflow:
            workflow["year"] = args.year or get_current_year()
        if "country_code" not in workflow:
            workflow["country_code"] = args.country_code
        
        # Формат экспорта всегда parquet, игнорируем иные настройки
        if "export" not in workflow:
            workflow["export"] = {}
        workflow["export"]["format"] = "parquet"
        
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
