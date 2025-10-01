import re
import polars as pl
from pathlib import Path
import tempfile
import logging
from .config import REGEX_PATTERNS_UNICODE, APP_TITLE
import hashlib

logger = logging.getLogger(APP_TITLE)


class PatternDetector:
    def __init__(self):
        self.regex_patterns = REGEX_PATTERNS_UNICODE
        self.compiled_patterns = {
            name: re.compile(pattern) for name, pattern in self.regex_patterns.items()
        }
        combined = '|'.join(f"(?P<{name}>{pattern})" for name, pattern in self.regex_patterns.items())
        self.combined_regex = re.compile(combined)

    def detect_patterns(self, values: list[str]) -> set[str]:
        detected = set()
        for value in values:
            matches = self.combined_regex.finditer(value)
            for match in matches:
                detected.add(match.lastgroup)
        return detected

    def detect_patterns_polars(self, df: pl.DataFrame, column: str) -> set[str]:
        """
        Для колонки проверяем, какие паттерны встречаются хотя бы в одном элементе.
        Возвращаем set найденных ключей из regex_patterns.
        """
        detected = set()
        col = df[column].drop_nans().drop_nulls().cast(pl.Utf8)
        for name, pattern in self.regex_patterns.items():
            try:
                if col.str.contains(pattern, literal=False).any():
                    detected.add(name)
            except pl.ComputeError as e:
                logging.error(f"{e}")
                continue
        return detected

    def combine_regex(self, selected_keys: list) -> re.Pattern:
        patterns = [self.regex_patterns[key] for key in selected_keys if key in self.regex_patterns]
        return re.compile("|".join(patterns)) if patterns else None


def remove_all_quotes(path: Path, encoding="utf-8") -> Path:
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    with path.open("r", encoding=encoding, errors="ignore") as infile, open(temp_file.name, "w",
                                                                            encoding="utf-8") as outfile:
        for line in infile:
            outfile.write(line.replace('"', '').replace("'", ""))
    return Path(temp_file.name)


def file_contains_quotes(path: Path, encoding="utf-8", max_lines: int = 20) -> bool:
    try:
        with path.open("r", encoding=encoding, errors="ignore") as f:
            for i, line in enumerate(f):
                if '"' in line or "'" in line:
                    return True
                if i >= max_lines:
                    break
    except Exception as e:
        logger.warning(f"Failed to check quotes in file {path}: {e}")
    return False


def clean_special_chars(ldf: pl.LazyFrame) -> pl.LazyFrame:
    cols = ldf.collect_schema().names()
    for col in cols:
        ldf = ldf.with_columns([
            pl.col(col).cast(pl.Utf8).alias(col)
        ])

    rules = [
        (r"&nbsp;|\\n|\\t|\\r|\xa0|\ufeff", " "),
        (r"[\n\r\t]", " "),
        (r"[\u00a0\u202f\u2007\u1680\u180e\u205f]", " "),
        (r"[\u200b\u200c\u200d\u2060\u00ad\u200e\u200f\u061c]", ""),
        (r"[\x00-\x1F\x7F]", "")
    ]

    for pattern, repl in rules:
        ldf = ldf.with_columns([
            pl.col(col).str.replace_all(pattern, repl).alias(col)
            for col in cols
        ])

    return ldf


def normalize_whitespace(ldf: pl.LazyFrame) -> pl.LazyFrame:
    return ldf.with_columns([
        pl.col(col)
        .cast(pl.Utf8)
        .str.replace_all(r"\s{2,}", " ")
        .str.strip_chars()
        .alias(col)
        for col in ldf.collect_schema().names()
    ])


def replace_empty_with(ldf: pl.LazyFrame, value: str | None = None) -> pl.LazyFrame:
    empty_values = ["", " ", "nan", "nan", "NaN", "none", "None", "null", "NULL", "0"]

    for col in ldf.collect_schema().names():
        ldf = ldf.with_columns(
            pl.when(
                pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(empty_values) | pl.col(col).is_null()
            )
            .then(None if value is None else value)
            .otherwise(pl.col(col))
            .alias(col)
        )
    return ldf


def drop_null_rows(ldf: pl.LazyFrame, required_columns: list[str]) -> pl.LazyFrame:
    if not required_columns:
        return ldf
    return ldf.drop_nulls(subset=required_columns)


def drop_duplicates(ldf: pl.LazyFrame, unique_columns: list[str]) -> pl.LazyFrame:
    """
    Удаляет дубликаты по указанным колонкам.
    Если список колонок пуст, использует все колонки по умолчанию.
    """
    if not unique_columns:
        # Если колонки не указаны, используем все колонки
        schema_cols = list(ldf.collect_schema().names())
        return ldf.unique(subset=schema_cols)
    return ldf.unique(subset=unique_columns)


def lowercase_columns(ldf: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame:
    return ldf.with_columns([
        pl.col(col).str.to_lowercase().alias(col)
        for col in columns
    ])


def apply_regex_patterns(
        ldf: pl.LazyFrame,
        column: str,
        regex_pattern: str,
) -> pl.LazyFrame:
    if regex_pattern:
        ldf = ldf.with_columns(
            pl.col(column)
            .fill_null("")
            .cast(str)
            .str.extract_all(regex_pattern)
            .map_elements(lambda matches: "".join(matches), return_dtype=pl.String)
            .alias(column)
        )
    return ldf


def short_hash(s: str, length=20) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:length]
