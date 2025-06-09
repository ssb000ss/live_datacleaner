import csv
import dask.dataframe as dd
import chardet
import streamlit as st

class FileManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.encoding = None
        self.delimiter = None
        self.df = None

    def detect_delimiter(self):
        try:
            with open(self.file_path, 'r', encoding=self.encoding or 'utf-8') as f:
                sample = f.read(10000)
                sniffer = csv.Sniffer()
                self.delimiter = sniffer.sniff(sample).delimiter
        except Exception:
            self.delimiter = ','

    def detect_file(self, sample_size=5000):
        with open(self.file_path, "rb") as f:
            rawdata = f.read(sample_size)
            info = chardet.detect(rawdata)
            self.encoding = info['encoding'] or 'utf-8'

    def load_data(self):
        try:
            self.detect_file()
            self.detect_delimiter()
            self.df = dd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                dtype=str,
                assume_missing=True,
                blocksize="100MB",
            )
            return self.df.head(1_000), self.encoding, self.delimiter
        except UnicodeDecodeError:
            st.error("Ошибка кодировки! Попробуйте другую кодировку.")
        except ValueError as e:
            st.error(f"Ошибка при разборе CSV: {e}")
        except MemoryError:
            st.error("Ошибка памяти! Попробуйте загрузить меньший файл.")
        except Exception as e:
            st.error(f"Ошибка загрузки файла: {e}")
        return None, None, None