import re
import unicodedata
import chardet
from urllib.parse import urlparse, parse_qs, urlunparse, unquote

#
# 1. УТИЛІТНІ ФУНКЦІЇ
#

def normalize_text(text):
    """Нормалізує текст: нижній регістр, видалення діакритики та зайвих пробілів."""
    if not text: return ""
    try:
        # NFKD розкладає символи на базові та комбінувальні знаки
        nfkd_form = unicodedata.normalize('NFKD', str(text).lower())
        # Фільтруємо символи, що не є пробілами (видаляємо діакритику)
        normalized = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
        # Нормалізуємо пробіли (видаляємо зайві)
        return " ".join(normalized.split())
    except Exception:
        # У випадку помилки повертаємо оригінальний текст у нижньому регістрі
        return str(text).lower().strip()

def normalize_url(url_string):
    """Нормалізує URL, додаючи слеш до кореневого шляху, якщо він відсутній."""
    if not url_string: return url_string
    try:
        parsed = urlparse(url_string)
        # Додаємо слеш, якщо шлях порожній (тільки домен)
        path = parsed.path if parsed.path else '/'
        # Збираємо URL назад
        normalized = urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, parsed.query, parsed.fragment))
        return normalized
    except Exception:
        # У випадку помилки парсингу повертаємо оригінальний URL
        return url_string

def extract_sheet_params(url):
    """Витягує ID таблиці та ID вкладки (gid) з URL Google таблиці."""
    sheet_id_match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    if not sheet_id_match: return None, None

    # Витягуємо gid з URL параметрів (пріоритети: 1-fragment, 2-URL параметр, 3-перша вкладка)
    parsed_url = urlparse(url)
    fragment, query = parsed_url.fragment.replace('gid=', ''), parse_qs(parsed_url.query)
    gid = int(fragment) if fragment.isdigit() else int(query.get('gid', [0])[0]) if query.get('gid', [''])[0].isdigit() else 0

    return sheet_id_match.group(1), gid

def is_ssl_error(error_text):
    """Перевіряє, чи пов'язана помилка з проблемами SSL."""
    return any(keyword.lower() in error_text.lower() for keyword in
              ['ssl', 'certificate', 'cert', 'handshake', 'verify', 'verification', 'CERTIFICATE_VERIFY_FAILED'])

def detect_encoding(html_content_bytes):
    """Визначає кодування HTML-контенту."""
    # Спробуємо визначити кодування за допомогою chardet
    detected = chardet.detect(html_content_bytes)
    encoding = detected['encoding']

    # Якщо визначено як ascii, але є кириличні символи, використовуємо windows-1251
    if encoding == 'ascii' and any(b >= 0xC0 for b in html_content_bytes):
        encoding = 'windows-1251'

    # Перевіряємо чи знайдено валідне кодування, інакше використовуємо utf-8
    return encoding if encoding else 'utf-8' 