import logging
from logging import FileHandler, StreamHandler
from pathlib import Path
import time
from colorama import init, Fore, Style


def init_logger(log_folder: Path, app_name: str) -> logging.Logger:
    """
    Инициализация логгера с файловым и консольным обработчиками.

    Args:
        log_folder (Path): Папка для хранения логов.
        app_name (str): Имя логгера.

    Returns:
        logging.Logger: Настроенный экземпляр логгера.
    """
    # Инициализация colorama для кроссплатформенного цветного вывода
    init()

    # Получаем логгер
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)

    # Очищаем существующие обработчики
    if logger.hasHandlers():
        logger.handlers.clear()

    # Отключаем распространение сообщений в корневой логгер, чтобы избежать дублирования
    logger.propagate = False

    # Создаём папку для логов
    log_folder.mkdir(parents=True, exist_ok=True)

    # Создаём файл логов с временной меткой
    date_str = time.strftime("%d_%m_%Y_%H_%M_%S")
    log_file_name = f"{date_str}.log"
    log_file_path = log_folder / log_file_name

    # Форматтер для файла (без цветов)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Форматтер для консоли (с цветами)
    class ColoredFormatter(logging.Formatter):
        LEVEL_COLORS = {
            logging.DEBUG: Fore.CYAN,
            logging.INFO: Fore.GREEN,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Fore.RED + Style.BRIGHT,
        }

        def format(self, record):
            color = self.LEVEL_COLORS.get(record.levelno, Fore.WHITE)
            message = super().format(record)
            return f"{color}{message}{Style.RESET_ALL}"

    console_formatter = ColoredFormatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Файловый обработчик (логирует DEBUG и выше)
    file_handler = FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Консольный обработчик (логирует INFO и выше)
    console_handler = StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Логгер инициализирован с файлом: %s", log_file_path)
    return logger
