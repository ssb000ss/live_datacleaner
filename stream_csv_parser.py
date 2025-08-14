#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import chardet
import logging
from pathlib import Path
from typing import Generator, Tuple

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('streaming_csv_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def detect_encoding(file_path: Path, sample_size: int = 10000) -> str:
    """Определяет кодировку файла."""
    try:
        with open(file_path, "rb") as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result['encoding'] or 'utf-8'

            if encoding.lower() == 'ascii':
                encoding = 'utf-8'

            logger.info(f"✅ Определена кодировка: {encoding}")
            return encoding
    except Exception as e:
        logger.warning(f"⚠️ Ошибка определения кодировки: {e}. Используем utf-8")
        return 'utf-8'


def detect_delimiter(file_path: Path, encoding: str, max_lines: int = 30) -> str:
    """Определяет разделитель CSV."""
    try:
        with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
            lines = [f.readline() for _ in range(max_lines)]
            sample = ''.join(lines)

            # Используем встроенный CSV sniffer
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
                logger.info(f"✅ Определен разделитель: {repr(delimiter)}")
                return delimiter
            except csv.Error:
                # Fallback на частотный анализ
                delimiters = [',', ';', '\t', '|']
                counts = {d: sum(line.count(d) for line in lines) for d in delimiters}
                delimiter = max(counts, key=counts.get)
                logger.info(f"✅ Определен разделитель (частотный анализ): {repr(delimiter)}")
                return delimiter
    except Exception as e:
        logger.warning(f"⚠️ Ошибка определения разделителя: {e}. Используем запятую")
        return ','


def process_csv_streaming(input_path: Path, output_path: Path, bad_path: Path,
                          encoding: str, delimiter: str, export_delimiter: str = '~',
                          batch_size: int = 10000):
    """Обрабатывает CSV потоково для экономии памяти."""

    valid_count = 0
    bad_count = 0

    try:
        with open(input_path, 'r', encoding=encoding, errors='ignore', newline='') as infile, \
                open(output_path, 'w', encoding='utf-8', newline='') as outfile, \
                open(bad_path, 'w', encoding='utf-8', newline='') as badfile:

            # Создаем CSV reader с исходным разделителем
            reader = csv.reader(infile, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)

            # Создаем CSV writer с новым разделителем и правильным экранированием
            writer = csv.writer(outfile, delimiter=export_delimiter, quoting=csv.QUOTE_ALL)
            bad_writer = csv.writer(badfile, delimiter=export_delimiter, quoting=csv.QUOTE_ALL)

            # Записываем заголовок в bad файл с описанием колонок
            bad_header = ['Номер_строки', 'Тип_ошибки', 'Описание_ошибки', 'Содержимое_строки']
            bad_writer.writerow(bad_header)

            # Обрабатываем заголовок
            try:
                header = next(reader)
                writer.writerow(header)
                expected_columns = len(header)
                logger.info(f"📋 Заголовок: {expected_columns} столбцов")
                logger.info(f"🔧 Новый разделитель: {repr(export_delimiter)}")
            except StopIteration:
                logger.error("❌ Файл пустой или не содержит заголовок")
                return

            # Обрабатываем строки потоково
            for row_num, row in enumerate(reader, start=2):
                try:
                    if len(row) == expected_columns:
                        # Валидная строка
                        writer.writerow(row)
                        valid_count += 1

                        if valid_count % batch_size == 0:
                            logger.info(f"✅ Обработано валидных строк: {valid_count}")

                    else:
                        # Неправильное количество столбцов
                        error_desc = f"Неверное количество столбцов: {len(row)} вместо {expected_columns}"
                        bad_writer.writerow([row_num, "Ошибка_структуры", error_desc, delimiter.join(row)])
                        bad_count += 1

                        if bad_count % 1000 == 0:
                            logger.warning(f"⚠️ Невалидных строк: {bad_count}")

                except Exception as e:
                    # Ошибка при обработке строки
                    error_desc = f"Ошибка обработки: {str(e)[:100]}"
                    bad_writer.writerow([row_num, "Ошибка_обработки", error_desc, delimiter.join(row)])
                    bad_count += 1

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise

    logger.info(f"✅ Обработка завершена. Валидных: {valid_count}, Невалидных: {bad_count}")
    return valid_count, bad_count


def main():
    parser = argparse.ArgumentParser(description='Потоковый CSV парсер для больших файлов с редким разделителем')
    parser.add_argument('input_path', help='Путь к входному CSV файлу')
    parser.add_argument('output_path', help='Путь для сохранения очищенного CSV')
    parser.add_argument('bad_path', help='Путь для сохранения невалидных строк')
    parser.add_argument('--encoding', help='Кодировка файла (автоопределение по умолчанию)')
    parser.add_argument('--delimiter', help='Разделитель входного CSV (автоопределение по умолчанию)')
    parser.add_argument('--export_delimiter', default='~', help='Разделитель для выходного файла (по умолчанию: ~)')
    parser.add_argument('--sample_size', type=int, default=10000,
                        help='Размер выборки для определения кодировки')
    parser.add_argument('--batch_size', type=int, default=10000,
                        help='Размер батча для логирования прогресса')

    args = parser.parse_args()

    input_path = Path(args.input_path)
    output_path = Path(args.output_path)
    bad_path = Path(args.bad_path)

    if not input_path.exists():
        logger.error(f"❌ Входной файл не найден: {input_path}")
        return

    try:
        # Определяем параметры файла
        encoding = args.encoding or detect_encoding(input_path, args.sample_size)
        delimiter = args.delimiter or detect_delimiter(input_path, encoding)
        export_delimiter = args.export_delimiter

        logger.info(f"🔍 Параметры: encoding={encoding}, входной разделитель={repr(delimiter)}")
        logger.info(f"🔧 Выходной разделитель: {repr(export_delimiter)}")
        logger.info(f"📁 Размер файла: {input_path.stat().st_size / (1024 * 1024):.1f} MB")

        # Обрабатываем CSV
        valid_count, bad_count = process_csv_streaming(
            input_path, output_path, bad_path, encoding, delimiter, export_delimiter, args.batch_size
        )

        logger.info("✅ Обработка CSV завершена успешно!")

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
