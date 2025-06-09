# utils/data_cleaner.py
import re
from .config import REGEX_PATTERNS

class PatternDetector:
    def __init__(self):
        self.regex_patterns = REGEX_PATTERNS
        self.compiled_patterns = {name: re.compile(pattern) for name, pattern in self.regex_patterns.items()}
        combined_pattern = '|'.join(f'(?P<{name}>{pattern})' for name, pattern in self.regex_patterns.items())
        self.combined_regex = re.compile(combined_pattern)

    def detect_patterns(self, column_values):
        detected = set()
        for value in column_values:
            value = value.lower().strip()
            matches = self.combined_regex.finditer(value)
            for match in matches:
                detected.add(match.lastgroup)
        return detected

    def format_column(self, df, column, patterns_to_keep):
        patterns_to_remove = [p for p in self.regex_patterns if p not in patterns_to_keep]
        remove_pattern = ''.join([f'[^{self.regex_patterns[p]}]' for p in patterns_to_remove])
        return [re.sub(remove_pattern, '', str(value)) for value in df[column]]

    def combine_regex(self, selected_keys: list) -> re.Pattern:
        patterns = [self.regex_patterns[key] for key in selected_keys if key in self.regex_patterns]
        return re.compile("|".join(patterns)) if patterns else None