import sys

if len(sys.argv) > 1:
    google_sheet_url = sys.argv[1]
    print(f"Отримано URL Google Sheet: {google_sheet_url}")
    # Тепер ви можете використовувати змінну google_sheet_url у вашому скрипті
else:
    print("Помилка: URL Google Sheet не передано як аргумент командного рядка.")
    sys.exit(1)

# Встановлення необхідних бібліотек тільки якщо вони відсутні
import importlib.util
import sys
import subprocess

# Автоматично встановлюємо відсутні пакети
def install_missing_packages(packages):
    for pkg in packages:
        spec = importlib.util.find_spec(pkg)
        if spec is None:
            print(f"Встановлюємо {pkg}...")
            try:
                # Використовуємо subprocess для надійного встановлення
                subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                print(f"{pkg} встановлено успішно.")
            except subprocess.CalledProcessError as e:
                print(f"Помилка встановлення {pkg}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"Невідома помилка під час встановлення {pkg}: {e}", file=sys.stderr)

# Використовуємо try-except для імпорту в середовищі Colab
COLAB_ENV = False # За замовчуванням вважаємо, що не в Colab
try:
    # Імпорти для Colab
    from google.colab import auth
    from google.auth import default
    from IPython.utils.io import capture_output
    from IPython.display import clear_output
    COLAB_ENV = True
    print("Запущено в середовищі Google Colab.")
    install_missing_packages(["gspread", "pandas", "requests", "beautifulsoup4", "chardet"])
except ImportError:
    print("Запущено поза середовищем Google Colab. Перевірка наявності пакетів...")
    # Тут можна додати альтернативні імпорти або логіку для локального запуску, якщо потрібно
    # Наприклад, встановлення пакетів, якщо вони потрібні локально:
    install_missing_packages(["gspread", "pandas", "requests", "beautifulsoup4", "chardet"])
    pass # Наразі нічого не робимо, якщо не в Colab

# Імпорт основних функцій з модулів
from gsheet_utils import check_sheet_structure, display_sheet_validation_results, update_sheet_with_results
from request_processor import check_status_code_requests

#
# 6. ГОЛОВНА ФУНКЦІЯ
#
def main(google_sheet):
    """Головна функція, що запускає перевірку та виводить результати."""
    # Якщо в Colab, авторизуємося
    if COLAB_ENV:
        try:
            print("Авторизуємося в Google (Colab)...")
            auth.authenticate_user()
            print("Авторизація в Google пройшла успішно.")
        except Exception as auth_e:
             print(f"Помилка авторизації в Google: {auth_e}", file=sys.stderr)
             return # Зупиняємо виконання, якщо авторизація не вдалась

    # Якщо не в Colab, ми припускаємо, що автентифікація gspread оброблена (напр., через gcloud auth application-default login)
    elif not COLAB_ENV:
        print("Запуск поза Colab. Переконайтеся, що автентифікація gspread налаштована.")
        # Тут не потрібно явно викликати auth.authenticate_user()

    # Перевірка структури таблиці (викликає gspread.authorize всередині)
    result = check_sheet_structure(google_sheet)
    display_sheet_validation_results(result)

    if result["success"]:
        data = result["data"]
        headers = data[0]
        rows = data[1:]

        # Знаходимо індекси потрібних стовпців
        try:
            idx_url = headers.index("Url")
            idx_anchor1 = headers.index("Анкор-1")
            idx_url1 = headers.index("Урл-1")
            # Додаємо обробку можливої відсутності Анкор-2/Урл-2 та Анкор-3/Урл-3
            idx_anchor2 = headers.index("Анкор-2") if "Анкор-2" in headers else -1
            idx_url2 = headers.index("Урл-2") if "Урл-2" in headers else -1
            idx_anchor3 = headers.index("Анкор-3") if "Анкор-3" in headers else -1
            idx_url3 = headers.index("Урл-3") if "Урл-3" in headers else -1
        except ValueError as e:
            print(f"Помилка: Не знайдено обов'язковий стовпець ('Анкор-1', 'Урл-1', 'Url', або опціональні 'Анкор-2/3', 'Урл-2/3') у заголовках: {e}")
            return

        # Формуємо список словників для передачі в check_status_code_requests
        rows_to_check = []
        for row_idx, row in enumerate(rows, 2): # Починаємо нумерацію рядків з 2 для повідомлень
            # Перевіряємо, чи рядок достатньо довгий для зчитування *обов'язкових* полів
            min_required_len = max(idx_anchor1, idx_url1, idx_url) + 1
            if len(row) < min_required_len:
                 print(f"Попередження: Рядок {row_idx}: Пропускаємо короткий рядок (менше {min_required_len} стовпців): {row}")
                 continue

            row_data = {
                "Анкор-1": row[idx_anchor1],
                "Урл-1": row[idx_url1],
                # Додаємо Анкор/Урл 2 і 3 з перевіркою індексу та довжини рядка
                "Анкор-2": row[idx_anchor2] if idx_anchor2 != -1 and idx_anchor2 < len(row) else None,
                "Урл-2": row[idx_url2] if idx_url2 != -1 and idx_url2 < len(row) else None,
                "Анкор-3": row[idx_anchor3] if idx_anchor3 != -1 and idx_anchor3 < len(row) else None,
                "Урл-3": row[idx_url3] if idx_url3 != -1 and idx_url3 < len(row) else None,
                "Url": row[idx_url]
            }
            # Додаємо тільки якщо є URL для перевірки
            if row_data["Url"]:
                rows_to_check.append(row_data)
            else:
                 print(f"Попередження: Рядок {row_idx}: Порожній 'Url', пропускаємо.")

        if not rows_to_check:
            print("Не знайдено жодного URL для перевірки в таблиці.")
            return

        check_results = check_status_code_requests(rows_to_check)

        update_sheet_with_results(result["worksheet"], check_results)

# Перевірка Google таблиці
google_sheet = google_sheet

# Запуск головної функції
if __name__ == "__main__":
    main(google_sheet)
