import logging
from pathlib import Path
from typing import Optional
import polars as pl
import time
import shutil

from utils import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(config.APP_TITLE)


class DataLoader:
    def __init__(self, parquet_folder_path: Path):
        self.parquet_folder = parquet_folder_path
        self.parquet_folder.mkdir(parents=True, exist_ok=True)

    def read_csv_lazy(self, path: Path, encoding: str = "utf8", delimiter: str = ",") -> Optional[pl.LazyFrame]:
        if not path.is_file():
            logger.error(f"File does not exist or is not a file: {path}")
            return None

        try:
            lazy_df = pl.read_csv(
                path,
                encoding=encoding,
                separator=delimiter,
                ignore_errors=True,
                null_values=["", " ", "\t", "NULL", "null", "NaN", "nan", "None", "none"],
                truncate_ragged_lines=True,
                quote_char=None,
                infer_schema_length=10000
            ).lazy()


            schema = lazy_df.collect_schema()
            string_cols = [col for col in schema if schema[col] == pl.Utf8]

            if string_cols:
                lazy_df = lazy_df.with_columns([
                    pl.col(c).str.replace_all('"', '').alias(c) for c in string_cols
                ])

            logger.info(f"Successfully read and cleaned CSV: {path}")
            return lazy_df

        except Exception as e:
            logger.error(f"Failed to read CSV {path}: {e}")
            return None

    def get_parquet_dir(self, source_path: Path) -> Path:
        parquet_dir = self.parquet_folder / source_path.stem
        parquet_dir.mkdir(parents=True, exist_ok=True)
        return parquet_dir

    def get_parquet_path(self, source_path: Path) -> Path:
        return self.parquet_folder / f"{source_path.stem}.parquet"

    def save_lazy_to_parquet(self, lazy_df: pl.LazyFrame, parquet_path: Path) -> bool:
        try:
            lazy_df.sink_parquet(str(parquet_path), compression="zstd")
            logger.info(f"Successfully saved parquet: {parquet_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save parquet {parquet_path}: {e}")
            return False

    def read_parquet_lazy(self, parquet_path: Path) -> Optional[pl.LazyFrame]:
        if not parquet_path.is_file():
            logger.warning(f"Parquet file does not exist: {parquet_path}")
            return None
        try:
            lazy_df = pl.read_parquet(str(parquet_path)).lazy()
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
