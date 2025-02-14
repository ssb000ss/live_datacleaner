import csv
import dask.dataframe as dd
import chardet
import streamlit as st


def detect_delimiter(file_path: str, encoding: str = 'utf-8') -> str:
    """Определяет разделитель CSV-файла"""
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            sample = f.read(10_000)
            sniffer = csv.Sniffer()
            return sniffer.sniff(sample).delimiter
    except Exception:
        return ','


def detect_file(file_path, sample_size=5000):
    """Определяет кодировку файла."""
    with open(file_path, "rb") as f:
        rawdata = f.read(sample_size)
        info = chardet.detect(rawdata)
        info['encoding'] = info['encoding'] or 'utf-8'
    return info


def load_data(file_path):
    """Загружает CSV-файл через Dask с автоопределением разделителя и кодировки"""
    try:
        info = detect_file(file_path)
        encoding = info['encoding']
        delimiter = detect_delimiter(file_path, encoding)
        lang = info['language']

        st.write(f"📌 Обнаружен разделитель: `{delimiter}` | Кодировка: `{encoding}` | Язык: {lang}", )

        df = dd.read_csv(
            file_path,
            delimiter=delimiter,
            encoding=encoding,
            dtype=str,
            assume_missing=True,
            blocksize="100MB",
        )

        df_sample = df.head(10_000)
        return df_sample

    except UnicodeDecodeError:
        st.error("Ошибка кодировки! Попробуйте другую кодировку.")
    except ValueError as e:
        st.error(f"Ошибка при разборе CSV: {e}")
    except MemoryError:
        st.error("Ошибка памяти! Попробуйте загрузить меньший файл.")
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {e}")

    return None
