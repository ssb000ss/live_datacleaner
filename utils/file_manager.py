import csv
import chardet
import logging
from utils import config
from pathlib import Path

logger = logging.getLogger(config.APP_TITLE)


class FileManager:
    def __init__(self):
        self.encoding = None
        self.delimiter = None

    def detect_delimiter(self, file_path: Path):
        with open(file_path, 'r', encoding=self.encoding or 'utf8') as f:
            logger.info(f"Detecting delimiter [{file_path.name}].")
            sample = f.read(5000)
            sniffer = csv.Sniffer()
            self.delimiter = sniffer.sniff(sample).delimiter

    def detect_file(self, file_path: Path, sample_size=5000):
        with open(file_path, "rb") as f:
            logger.info(f"Detecting encoding [{file_path.name}].")
            rawdata = f.read(sample_size)
            info = chardet.detect(rawdata)
            detected_encoding = info['encoding'] or "utf-8"
            
            # Конвертируем кодировки для совместимости с Polars
            if detected_encoding.lower() in ['utf-8', 'utf8']:
                self.encoding = "utf8"
            elif detected_encoding.lower() == 'ascii':
                self.encoding = "utf8"  # ASCII является подмножеством UTF-8
            else:
                self.encoding = "utf8-lossy"  # Для других кодировок используем lossy режим

    def get_extension(self, file_path: Path):
        return f".{file_path.name.rsplit('.', 1)[-1].lower()}" if '.' in file_path.name else ""

    def load_data(self, file_path: Path):
        try:
            self.detect_file(file_path)
            self.detect_delimiter(file_path)
            return file_path, self.encoding, self.delimiter, self.get_extension(file_path)
        except UnicodeDecodeError:
            logger.error("Ошибка кодировки! Попробуйте другую кодировку.")
        except ValueError as e:
            logger.error(f"Ошибка при разборе CSV: {e}")
        except MemoryError:
            logger.error("Ошибка памяти! Попробуйте загрузить меньший файл.")
        except Exception as e:
            logger.error(f"Ошибка загрузки файла: {e}")
        return None, None, None
