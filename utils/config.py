PATTERN_DISPLAY_MAP = {
    "digits": "Цифры",
    "colon_symbol": ":",
    "hyphen_symbol": "-",
    "underscore_symbol": "_",
    "period": ".",
    "comma_symbol": ",",
    "space": "Пробел",
    "backslash": "\\",
    "vertical_bar": "|",
    "forward_slash": "/",
    "double_quote": '"',
    "single_quote": "'",
    "dollar": "$",
    "at": "@",
    "hash_symbol": "#",
    "asterisk": "*",
    "semicolon_symbol": ";",
    "escaped_space": "\\s",
    "escaped_newline": "\\n",
    "cyrillic": "Кириллица",
    "latin": "Латиница",
    "kazakh_cyrillic": "Казахская кириллица",
    "kyrgyz_cyrillic": "Киргизская кириллица",
    "uzbek_cyrillic": "Узбекская кириллица",
    "kazakh_latin": "Казахская латиница",
    "kyrgyz_latin": "Киргизская латиница",
    "uzbek_latin": "Узбекская латиница",
}

# Регулярные выражения для PatternDetector
REGEX_PATTERNS = {
    "kazakh_cyrillic": r'[әӘғҒқҚңҢөӨұҰүҮіІ]',
    "kazakh_latin": r'[äÄğĞıİñÑöÖşŞüÜ]',
    "kyrgyz_cyrillic": r'[өӨүҮҢң]',
    "kyrgyz_latin": r'[öÖüÜ]',
    "uzbek_cyrillic": r'[ӯӮқҚғҒ]',
    "latin": r'[a-zA-Z]',
    "cyrillic": r'[а-яА-ЯёЁ]',
    "digits": r'\d',
    "colon_symbol": r":",
    "hyphen_symbol": r"-",
    "underscore_symbol": r"_",
    "period": r"\.",
    "comma_symbol": r",",
    "space": r'\s',
    "backslash": r"\\",
    "vertical_bar": r"\|",
    "forward_slash": r"/",
    "double_quote": r'"',
    "single_quote": r"'",
    "dollar": r"\$",
    "at": r"@",
    "hash_symbol": r"#",
    "asterisk": r"\*",
    "semicolon_symbol": r";",
    "escaped_space": r"\\s",
    "escaped_newline": r"\\n"
}

# Список шагов для stepper_bar
STEPS = [
    "Загрузка файла",
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
APP_TITLE = "Интерактивная очистка CSV"
PAGE_ICON = "📊"
CSS_PATH = "static/style.css"
