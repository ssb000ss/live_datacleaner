import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

def get_env_path(key: str, default: str) -> Path:
    """Получает путь из переменной окружения и возвращает как Path объект"""
    value = os.getenv(key, default)
    path = Path(value)
    
    # Если путь относительный, делаем его относительно BASE_FOLDER
    if not path.is_absolute():
        base_folder = Path(os.getenv('BASE_FOLDER', '.')).resolve()
        path = base_folder / path
    
    return path.resolve()

def get_env_list(key: str, default: str, separator: str = ',') -> list:
    """Получает список из переменной окружения"""
    value = os.getenv(key, default)
    return [item.strip() for item in value.split(separator) if item.strip()]

def get_env_set(key: str, default: str, separator: str = ',') -> set:
    """Получает множество из переменной окружения"""
    return set(get_env_list(key, default, separator))

REGEX_PATTERNS_UNICODE = {
    # Кириллица
    "kazakh_cyrillic": r"[\u04D8\u04D9\u0406\u0456\u04B0\u04B1]",  # Әә, Іі, Ұұ
    "uzbek_cyrillic": r"[\u040E\u045E\u04B2\u04B3]",  # Ўў, Ҳҳ
    "cyrillic_common": r"[\u0410-\u044F\u0401\u0451]",  # А-Я, а-я, Ёё
    "cyrillic_extended": r"[\u04E8\u04E9\u04AF\u04B1\u04A2\u04A3\u049A\u049B\u0492\u0493]",  # Өө, Үү, Ңң, Ққ, Ғғ

    # Латиница
    "latyn_kazakh": r"[\u00E4\u00C4\u011F\u011E\u0131\u0130\u00F1\u00D1\u015F\u015E]",  # äÄ, ğĞ, ıİ, ñÑ, şŞ
    "latyn_uzbek": r"[\u02BB\u02BC]",  # ʼ
    "latin_basic": r"[A-Za-z]",  # A–Z, a–z
    "latin_extended": r"[\u00F6\u00D6\u00FC\u00DC]",  # öÖ, üÜ

    # Цифры
    "digits": r"[0-9]",  # 0–9

    # Пробелы и управляющие символы
    "space": r"\u0020",  # пробел
    "newline": r"\u000A",  # LF
    "literal_escaped_space": r"\\u0020",  # буквальная "\u0020"
    "literal_escaped_newline": r"\\u000A",  # буквальная "\u000A"

    # Пунктуация
    "colon": r":",  # :
    "semicolon": r";",  # ;
    "hyphen": r"-",  # -
    "underscore": r"_",  # _
    "period": r"\.",  # .
    "comma": r",",  # ,

    # Разделители
    "backslash": r"\\",  # \
    "forward_slash": r"/",  # /
    "vertical_bar": r"\|",  # |

    # Спецсимволы
    "double_quote": r'"',  # "
    "single_quote": r"'",  # '
    "dollar": r"\$",  # $
    "at": r"@",  # @
    "hash": r"#",  # #
    "asterisk": r"\*"  # *
}

PATTERN_DISPLAY_MAP_UNICODE = {
    # Кириллица
    "kazakh_cyrillic": "Казахская кириллица (Әә, Іі, Ұұ)",
    "uzbek_cyrillic": "Узбекская кириллица (Ўў, Ҳҳ)",
    "cyrillic_common": "Кириллица (А-Я, а-я, Ёё)",
    "cyrillic_extended": "Кириллица (Өө, Үү, Ңң, Ққ, Ғғ)",

    # Латиница
    "latyn_kazakh": "Казахская латиница (äÄ, ğĞ, ıİ, ñÑ, şŞ)",
    "latyn_uzbek": "Узбекская латиница (ʼ)",
    "latin_basic": "Латиница (A–Z, a–z)",
    "latin_extended": "Латиница (öÖ, üÜ)",

    # Цифры
    "digits": "Цифры (0–9)",

    # Пробелы и управляющие символы
    "space": "Пробел",
    "newline": "Новая строка",
    "literal_escaped_space": "Экранированный пробел (\\u0020)",
    "literal_escaped_newline": "Экранированный перенос строки (\\u000A)",

    # Пунктуация
    "colon": ":",
    "semicolon": ";",
    "hyphen": "-",
    "underscore": "_",
    "period": ".",
    "comma": ",",

    # Разделители
    "backslash": "\\",
    "forward_slash": "/",
    "vertical_bar": "|",

    # Спецсимволы
    "double_quote": '"',
    "single_quote": "'",
    "dollar": "$",
    "at": "@",
    "hash": "#",
    "asterisk": "*"
}

# Список шагов для stepper_bar
STEPS = [
    "Загрузка файла",
    "Анализ колонок",
    "Исключение ненужных колонок",
    "Конкатенация колонок",
    "Обработка колонок",
    "Обработка содержимого",
    "Удаление дубликатов и валидация",
    "Экспорт"
]

DEFAULT_COLUMN_NAMES = [
    "fio",
    "nickname",
    "number_phone",
    "email",
    "car_number",
    "address",
    "password",
    "birthday",
]

# Основные настройки приложения
APP_TITLE = os.getenv('APP_TITLE', 'LiveDataCleaner')
PAGE_ICON = os.getenv('PAGE_ICON', '📊')
CSS_PATH = os.getenv('CSS_PATH', 'static/style.css')
ANALYZE_ROWS = int(os.getenv('ANALYZE_ROWS', '1000'))

# Основные папки (настраиваются через .env)
BASE_FOLDER = get_env_path('BASE_FOLDER', '.')
INPUT_FOLDER = get_env_path('INPUT_FOLDER', 'data')
LOG_FOLDER = get_env_path('LOG_FOLDER', 'logs')
PARQUET_FOLDER = get_env_path('PARQUET_FOLDER', 'parquet_cache')

# Дополнительные папки
WORKFLOWS_FOLDER = get_env_path('WORKFLOWS_FOLDER', 'workflows')
EXPORTS_FOLDER = get_env_path('EXPORTS_FOLDER', 'exports')
ANALYZE_CACHE_FOLDER = get_env_path('ANALYZE_CACHE_FOLDER', 'analyze_cache')
STATIC_FOLDER = get_env_path('STATIC_FOLDER', 'static')

# Временная папка (опционально)
TEMP_FOLDER = get_env_path('TEMP_FOLDER', '') if os.getenv('TEMP_FOLDER') else None

# Поддерживаемые расширения файлов
COMPATIBLE_EXTENSIONS = get_env_list('COMPATIBLE_EXTENSIONS', '.csv,.txt')

# Коды стран для валидации
ALLOWED_COUNTRY_CODES = get_env_set('ALLOWED_COUNTRY_CODES', 'ru,kg,uz,tm,ua,by,nl,az')

# Transliteration mapping for Cyrillic to Latin
CYRILLIC_TO_LATIN_MAPPING = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "",
    "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    # Uppercase
    "А": "a", "Б": "b", "В": "v", "Г": "g", "Д": "d", "Е": "e", "Ё": "e",
    "Ж": "zh", "З": "z", "И": "i", "Й": "y", "К": "k", "Л": "l", "М": "m",
    "Н": "n", "О": "o", "П": "p", "Р": "r", "С": "s", "Т": "t", "У": "u",
    "Ф": "f", "Х": "kh", "Ц": "ts", "Ч": "ch", "Ш": "sh", "Щ": "shch", "Ъ": "",
    "Ы": "y", "Ь": "", "Э": "e", "Ю": "yu", "Я": "ya",
}


def ensure_directories() -> None:
    """Проверяет существование необходимых папок и создаёт их при отсутствии.
    Вызывается при импорте модуля, чтобы до запуска приложения все папки были готовы.
    """
    dirs = [
        INPUT_FOLDER,
        LOG_FOLDER,
        PARQUET_FOLDER,
        WORKFLOWS_FOLDER,
        EXPORTS_FOLDER,
        ANALYZE_CACHE_FOLDER,
        STATIC_FOLDER,
    ]
    # TEMP_FOLDER может быть None, создаём только если задан
    if TEMP_FOLDER:
        dirs.append(TEMP_FOLDER)

    for d in dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            # Не прерываем выполнение из-за ошибок прав доступа и т.п.
            pass


# Гарантируем создание папок при импорте конфигурации
ensure_directories()
