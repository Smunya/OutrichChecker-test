import os
import sys
# Додаємо кореневу папку у шлях імпорту, щоб pytest бачив модуль utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import utils
import types  # для створення простих «фейкових» об’єктів у тестах


# ------------------------ TEST normalize_text ------------------------

def test_empty_string():
    # Якщо на вході порожній рядок, очікуємо порожній рядок
    assert utils.normalize_text("") == ""


def test_none_input():
    # Якщо на вході None, метод повертає пустий рядок
    assert utils.normalize_text(None) == ""


def test_strip_and_lowercase():
    # Перевіряємо: обрізання зайвих пробілів та приведення до нижнього регістру
    text = "  Hello   World  "
    assert utils.normalize_text(text) == "Hello World"


def test_remove_diacritics():
    # Перевіряємо, що діакритичні знаки (акценти) видаляються
    assert utils.normalize_text("Café au lait") == "cafe au lait"
    assert utils.normalize_text("ÁÉÍÓÚ") == "aeiou"


def test_multiple_whitespace_types():
    # Табуляція, новий рядок та декілька пробілів мають зліпитись в один пробіл
    text = "Привіт\tсвіт\nце   тест"
    assert utils.normalize_text(text) == "привіт світ це тест"


def test_numeric_input():
    # Якщо передати число, воно конвертується в рядок
    assert utils.normalize_text(12345) == "12345"
    # Але 0 вважається «пустим» та повертається як порожній рядок
    assert utils.normalize_text(0) == ""


def test_exception_fallback(monkeypatch):
    # Імітуємо помилку в unicodedata.normalize, щоб перевірити блок except
    original_combining = utils.unicodedata.combining
    fake_unicodedata = types.SimpleNamespace(
        normalize=lambda form, s: (_ for _ in ()).throw(ValueError("normalize failed")),
        combining=original_combining
    )
    monkeypatch.setattr(utils, 'unicodedata', fake_unicodedata)
    # У разі помилки повертається початковий текст у нижньому регістрі, без зайвих пробілів
    assert utils.normalize_text("Тест Exception") == "тест exception"


def test_preserves_non_combining_chars():
    # Символи, які не є комбінуючими (цифри, знаки пунктуації), лишаються без змін
    assert utils.normalize_text("abc123!@#") == "abc123!@#"


# ------------------------ TEST normalize_url ------------------------


def test_normalize_url_empty_and_none():
    # Порожній рядок і None повинні повертатися без змін
    assert utils.normalize_url("") == ""
    assert utils.normalize_url(None) is None


def test_normalize_url_adds_trailing_slash():
    # Якщо URL без шляху, додаємо '/' після домену
    url = "http://example.com"
    assert utils.normalize_url(url) == "http://example.com/"


def test_normalize_url_preserves_existing_path():
    # Якщо шлях вже є, нічого не змінюємо
    url = "https://example.com/path/to/resource"
    assert utils.normalize_url(url) == url


def test_normalize_url_with_query_and_fragment():
    # Перевіряємо, що query і fragment залишаються, а slash ставиться перед '?' якщо було відсутнє
    url = "https://example.com?param=1#section"
    expected = "https://example.com/?param=1#section"
    assert utils.normalize_url(url) == expected


def test_normalize_url_exception_fallback(monkeypatch):
    # Якщо urlparse викидає помилку, повертаємо оригінальний рядок
    fake_parse = lambda x: (_ for _ in ()).throw(ValueError("parse error"))
    monkeypatch.setattr(utils, 'urlparse', fake_parse)
    assert utils.normalize_url("http://invalid_url") == "http://invalid_url"


# ------------------------ TEST extract_sheet_params ------------------------


def test_extract_sheet_params_no_match():
    # Некоректний URL без '/d/ID', повертаємо (None, None)
    assert utils.extract_sheet_params("https://docs.google.com/spreadsheets/") == (None, None)


def test_extract_sheet_params_only_id():
    # Є ID таблиці, але немає gid → gid = 0
    url = "https://docs.google.com/spreadsheets/d/abcd1234/edit"
    assert utils.extract_sheet_params(url) == ("abcd1234", 0)


def test_extract_sheet_params_with_gid_in_fragment():
    # gid у фрагменті '#gid=789' → пріоритет
    url = "https://docs.google.com/spreadsheets/d/abcd1234/edit#gid=789"
    assert utils.extract_sheet_params(url) == ("abcd1234", 789)


def test_extract_sheet_params_with_gid_in_query():
    # gid у параметрі query '?gid=456'
    url = "https://docs.google.com/spreadsheets/d/abcd1234/edit?gid=456"
    assert utils.extract_sheet_params(url) == ("abcd1234", 456)


def test_extract_sheet_params_fragment_priority_over_query():
    # І fragment, і query, але фрагмент переважає
    url = "https://docs.google.com/spreadsheets/d/abcd1234/edit?gid=456#gid=789"
    assert utils.extract_sheet_params(url) == ("abcd1234", 789)


def test_extract_sheet_params_fragment_non_numeric_query_non_numeric():
    # Нечислові значення у фрагменті й query → gid = 0 за замовчуванням
    url = "https://docs.google.com/spreadsheets/d/abcd1234/edit#gid=foo?gid=321"
    assert utils.extract_sheet_params(url) == ("abcd1234", 0)


# ------------------------ TEST is_ssl_error ------------------------


def test_is_ssl_error_positive():
    # При наявності будь-якого SSL-повʼязаного ключового слова маємо True
    cases = [
        "SSL handshake failed",
        "certificate verification error",
        "Cert not found",
        "Handshake error",
        "Verification failed",
        "Error: CERTIFICATE_VERIFY_FAILED"
    ]
    for c in cases:
        assert utils.is_ssl_error(c)


def test_is_ssl_error_negative():
    # Відмінні від SSL-помилок рядки → False
    cases = ["Connection refused", "Timeout occurred", "Some random error", ""]
    for c in cases:
        assert not utils.is_ssl_error(c)


# ------------------------ TEST detect_encoding ------------------------


def test_detect_encoding_with_utf8(monkeypatch):
    # Імітуємо, що chardet визначив 'utf-8'
    fake_detect = lambda b: {'encoding': 'utf-8'}
    monkeypatch.setattr(utils, 'chardet', types.SimpleNamespace(detect=fake_detect))
    assert utils.detect_encoding(b'<html></html>') == 'utf-8'


def test_detect_encoding_none(monkeypatch):
    # Якщо detect повертає None, використовуємо 'utf-8' за замовчуванням
    fake_detect = lambda b: {'encoding': None}
    monkeypatch.setattr(utils, 'chardet', types.SimpleNamespace(detect=fake_detect))
    assert utils.detect_encoding(b'\xff\xfe') == 'utf-8'


def test_detect_encoding_ascii_without_cyrillic(monkeypatch):
    # detect='ascii' без байтів >= 0xC0 → повертаємо 'ascii'
    fake_detect = lambda b: {'encoding': 'ascii'}
    monkeypatch.setattr(utils, 'chardet', types.SimpleNamespace(detect=fake_detect))
    assert utils.detect_encoding(b'Hello world!') == 'ascii'


def test_detect_encoding_windows1251_for_cyrillic(monkeypatch):
    # detect='ascii', але є байти кирилиці (>=0xC0) → 'windows-1251'
    fake_detect = lambda b: {'encoding': 'ascii'}
    monkeypatch.setattr(utils, 'chardet', types.SimpleNamespace(detect=fake_detect))
    data = bytes([0xC0, 0xC1, 0xD0, 0xE0])
    assert utils.detect_encoding(data) == 'windows-1251'
