import logging
from pathlib import Path
from typing import Optional, Iterator
import polars as pl
import time
import shutil
import psutil
import gc

from utils import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


class DataLoader:
    def __init__(self, parquet_folder_path: Path, chunk_size: int = 10000):
        self.parquet_folder = parquet_folder_path
        self.parquet_folder.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self.max_memory_usage = 0.8  # Максимальное использование памяти (80%)

    def _check_memory_usage(self) -> bool:
        """Проверяет использование памяти и возвращает True если можно продолжать"""
        memory_percent = psutil.virtual_memory().percent / 100
        if memory_percent > self.max_memory_usage:
            logger.warning(f"High memory usage: {memory_percent:.1%}, forcing garbage collection")
            gc.collect()
            memory_percent = psutil.virtual_memory().percent / 100
        return memory_percent <= self.max_memory_usage

    def read_csv_lazy(self, path: Path, encoding: str = "utf8", delimiter: str = ",") -> Optional[pl.LazyFrame]:
        if not path.is_file():
            logger.error(f"File does not exist or is not a file: {path}")
            return None

        if not self._check_memory_usage():
            memory_info = self.get_memory_usage()
            logger.error(f"Insufficient memory to load file. Memory usage: {memory_info['percent']:.1f}%")
            return None

        try:
            # Читаем файл с минимальными настройками для экономии памяти
            lazy_df = pl.scan_csv(
                path,
                encoding=encoding,
                separator=delimiter,
                ignore_errors=True,
                null_values=["", " ", "\t", "NULL", "null", "NaN", "nan", "None", "none"],
                truncate_ragged_lines=True,
                quote_char=None,
                infer_schema_length=1000,  # Уменьшено для экономии памяти
                rechunk=False  # Отключаем автоматическое переразбиение
            )

            # Получаем схему лениво, без загрузки данных
            schema = lazy_df.collect_schema()
            string_cols = [col for col in schema if schema[col] == pl.Utf8]

            # Применяем очистку строк лениво
            if string_cols:
                lazy_df = lazy_df.with_columns([
                    pl.col(c).str.replace_all('"', '').alias(c) for c in string_cols
                ])

            logger.info(f"Successfully read and cleaned CSV: {path}")
            return lazy_df

        except Exception as e:
            logger.error(f"Failed to read CSV {path}: {e}")
            if "encoding" in str(e).lower():
                logger.error(f"Encoding issue detected. Try using 'utf8' or 'utf8-lossy' instead of '{encoding}'")
                # Попробуем с utf8-lossy как fallback
                if encoding != "utf8-lossy":
                    logger.info("Trying with utf8-lossy encoding as fallback...")
                    try:
                        lazy_df = pl.scan_csv(
                            path,
                            encoding="utf8-lossy",
                            separator=delimiter,
                            ignore_errors=True,
                            null_values=["", " ", "\t", "NULL", "null", "NaN", "nan", "None", "none"],
                            truncate_ragged_lines=True,
                            quote_char=None,
                            infer_schema_length=1000,
                            rechunk=False
                        )
                        logger.info("Successfully read CSV with utf8-lossy encoding")
                        return lazy_df
                    except Exception as fallback_e:
                        logger.error(f"Fallback encoding also failed: {fallback_e}")
            return None

    def get_parquet_dir(self, source_path: Path) -> Path:
        parquet_dir = self.parquet_folder / source_path.stem
        parquet_dir.mkdir(parents=True, exist_ok=True)
        return parquet_dir

    def get_parquet_path(self, source_path: Path) -> Path:
        return self.parquet_folder / f"{source_path.stem}.parquet"

    def save_lazy_to_parquet(self, lazy_df: pl.LazyFrame, parquet_path: Path) -> bool:
        try:
            # Сохраняем с оптимизацией для больших файлов
            lazy_df.sink_parquet(
                str(parquet_path), 
                compression="zstd",
                compression_level=3,  # Баланс между размером и скоростью
                maintain_order=False  # Отключаем поддержание порядка для экономии памяти
            )
            logger.info(f"Successfully saved parquet: {parquet_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save parquet {parquet_path}: {e}")
            return False

    def read_parquet_lazy(self, parquet_path: Path) -> Optional[pl.LazyFrame]:
        if not parquet_path.is_file():
            logger.warning(f"Parquet file does not exist: {parquet_path}")
            return None
        
        if not self._check_memory_usage():
            memory_info = self.get_memory_usage()
            logger.error(f"Insufficient memory to load parquet file. Memory usage: {memory_info['percent']:.1f}%")
            return None
            
        try:
            # Используем scan_parquet для ленивой загрузки
            lazy_df = pl.scan_parquet(str(parquet_path))
            logger.info(f"Successfully read parquet: {parquet_path}")
            return lazy_df
        except Exception as e:
            logger.error(f"Failed to read parquet {parquet_path}: {e}")
            return None

    def clear_cache(self, source_path: Path):
        parquet_dir = self.get_parquet_dir(source_path)
        shutil.rmtree(parquet_dir, ignore_errors=True)
        logger.info(f"Cleared cache for {source_path}")

    def load_data(self, source_path: Path, encoding: str = "utf8", delimiter: str = ",") -> Optional[pl.LazyFrame]:
        parquet_path = self.get_parquet_path(source_path)

        start = time.perf_counter()
        logger.info(f"Attempting to read parquet file: {parquet_path}")
        lazy_df = self.read_parquet_lazy(parquet_path)
        if lazy_df is not None:
            logger.info(f"Loaded parquet in {time.perf_counter() - start:.2f} seconds")
            return lazy_df

        logger.info(f"Parquet not found or unreadable, reading CSV: {source_path}")
        lazy_df = self.read_csv_lazy(source_path, encoding=encoding, delimiter=delimiter)
        if lazy_df is None:
            return None

        logger.info(f"Saving parquet file: {parquet_path}")
        if not self.save_lazy_to_parquet(lazy_df, parquet_path):
            logger.warning(f"Failed to save parquet, proceeding with CSV data: {parquet_path}")

        logger.info(f"Loaded CSV and cached parquet in {time.perf_counter() - start:.2f} seconds")
        return lazy_df

    def process_data_in_chunks(self, lazy_df: pl.LazyFrame, operation_func, *args, **kwargs) -> Optional[pl.LazyFrame]:
        """Обрабатывает данные по частям для экономии памяти"""
        try:
            # Получаем общее количество строк лениво
            total_rows = lazy_df.select(pl.len()).collect().item()
            logger.info(f"Processing {total_rows} rows in chunks of {self.chunk_size}")
            
            if total_rows <= self.chunk_size:
                # Если данных мало, обрабатываем целиком
                return operation_func(lazy_df, *args, **kwargs)
            
            # Обрабатываем по частям
            processed_chunks = []
            for offset in range(0, total_rows, self.chunk_size):
                if not self._check_memory_usage():
                    logger.error("Insufficient memory to continue processing")
                    return None
                
                # Получаем чанк данных
                chunk = lazy_df.slice(offset, self.chunk_size)
                processed_chunk = operation_func(chunk, *args, **kwargs)
                
                if processed_chunk is not None:
                    processed_chunks.append(processed_chunk)
                
                # Принудительная очистка памяти после каждого чанка
                gc.collect()
                
                logger.info(f"Processed chunk {offset//self.chunk_size + 1}/{(total_rows + self.chunk_size - 1)//self.chunk_size}")
            
            if not processed_chunks:
                return None
                
            # Объединяем результаты
            result = processed_chunks[0]
            for chunk in processed_chunks[1:]:
                result = result.vstack(chunk)
                
            return result.lazy()
            
        except Exception as e:
            logger.error(f"Error processing data in chunks: {e}")
            return None

    def get_memory_usage(self) -> dict:
        """Возвращает информацию об использовании памяти"""
        memory = psutil.virtual_memory()
        return {
            'total': memory.total,
            'available': memory.available,
            'used': memory.used,
            'percent': memory.percent,
            'free': memory.free
        }
