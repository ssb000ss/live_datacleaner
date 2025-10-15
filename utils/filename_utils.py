"""
Utilities for filename generation and validation.
Based on data_loader implementation.
"""

import re
from pathlib import Path
from typing import Tuple, Optional
from datetime import datetime
from .config import ALLOWED_COUNTRY_CODES, CYRILLIC_TO_LATIN_MAPPING


def transliterate_cyrillic_to_latin(text: str) -> str:
    """Transliterate Cyrillic characters to Latin."""
    return "".join(CYRILLIC_TO_LATIN_MAPPING.get(ch, ch) for ch in text)


def validate_country_code(cc: str) -> Tuple[bool, Optional[str]]:
    """Validate country code."""
    if cc is None:
        return False, "Код страны не выбран"
    if cc.lower() not in ALLOWED_COUNTRY_CODES:
        return False, f"Код страны должен быть одним из: {', '.join(sorted(ALLOWED_COUNTRY_CODES))}"
    return True, None


def sanitize_basename(basename: str) -> str:
    """Sanitize filename basename for safe usage."""
    # Transliterate common Cyrillic to Latin first
    name = transliterate_cyrillic_to_latin(basename)
    # Lowercase
    name = name.lower()
    # Replace dots and commas with underscores
    name = name.replace(".", "_").replace(",", "_")
    # Replace spaces with underscore
    name = name.replace(" ", "_")
    # Replace any illegal characters with underscore (only allow a-z, 0-9, _, -)
    name = re.sub(r"[^a-z0-9_-]+", "_", name)
    # Collapse consecutive underscores
    name = re.sub(r"_+", "_", name)
    # Trim leading/trailing underscores/hyphens
    name = name.strip("_-")
    # Fallback if becomes empty
    return name or "data"


def generate_nomad_filename(
    country_code: str, 
    original_filename: str, 
    year: int, 
    version: str = "v1"
) -> str:
    """
    Generate filename in format: nomad-[код страны]-[название файла]-[год]-[версия].parquet
    
    Args:
        country_code: Country code (ru, cn, tm, kz, uz)
        original_filename: Original filename with extension
        year: Year (e.g., 2023)
        version: Version string (default: v1)
    
    Returns:
        Generated filename
    """
    # Validate country code
    is_valid, error = validate_country_code(country_code)
    if not is_valid:
        raise ValueError(error)
    
    # Get filename without extension and sanitize
    original_path = Path(original_filename)
    basename = original_path.stem
    extension = original_path.suffix
    
    # Sanitize the basename
    safe_basename = sanitize_basename(basename)
    
    # Combine with extension for the middle part
    middle_part = f"{safe_basename}{extension.replace('.', '_')}"
    
    # Generate final filename
    filename = f"nomad-{country_code.lower()}-{middle_part}-{year}-{version}.parquet"
    
    return filename


def validate_nomad_filename(filename: str) -> Tuple[bool, Optional[str]]:
    """Validate generated nomad filename format."""
    if not filename:
        return False, "Имя файла не должно быть пустым"
    
    # Check if it matches the expected pattern
    pattern = r"^nomad-[a-z]{2}-[a-z0-9_-]+-\d{4}-v\d+\.parquet$"
    if not re.match(pattern, filename):
        return False, "Имя файла не соответствует формату nomad-[код]-[название]-[год]-v[версия].parquet"
    
    # Extract country code and validate
    parts = filename.split("-")
    if len(parts) >= 2:
        country_code = parts[1]
        is_valid, error = validate_country_code(country_code)
        if not is_valid:
            return False, error
    
    return True, None


def get_current_year() -> int:
    """Get current year."""
    return datetime.now().year
