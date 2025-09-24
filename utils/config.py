from pathlib import Path

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
    # "Изменения regex",
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
APP_TITLE = "LiveDataCleaner"
PAGE_ICON = "📊"
CSS_PATH = "static/style.css"
BASE_FOLDER = Path(Path.cwd()).resolve()

INPUT_FOLDER = BASE_FOLDER / "data"
LOG_FOLDER = BASE_FOLDER / "logs"
ANALYZE_ROWS = 1_000

PARQUET_FOLDER = BASE_FOLDER / "parquet_cache"

COMPATIBLE_EXTENSIONS = ['.csv', '.txt']
