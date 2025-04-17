import pandas as pd
import gspread
import ast
from urllib.parse import unquote
from google.colab import auth
from google.auth import default

from utils import extract_sheet_params, normalize_url

#
# 4. ФУНКЦІЇ РОБОТИ З GOOGLE SHEETS
#
def check_sheet_structure(google_sheet):
    """Перевіряє структуру Google таблиці."""
    print("Авторизуємося в Google...")
    auth.authenticate_user()

    try:
        # Відкриття та перевірка таблиці
        print(f"Відкриваємо таблицю: {google_sheet}")
        sheet_params = extract_sheet_params(google_sheet)
        if not sheet_params:
            return {"success": False, "error": "Неправильний формат URL Google таблиці"}

        sheet_id, gid = sheet_params
        gc = gspread.authorize(default()[0])
        sheet = gc.open_by_key(sheet_id)

        # Отримання потрібної вкладки за gid
        all_worksheets = sheet.worksheets()
        worksheet = next((ws for ws in all_worksheets if ws.id == gid), None) or sheet.get_worksheet(0)
        print(f"{'Використовуємо вкладку: '+worksheet.title if worksheet.id == gid else f'Увага: Вкладка з gid={gid} не знайдена, використовуємо першу вкладку'}")

        # Перевірка даних та заголовків
        data = worksheet.get_all_values()
        if not data:
            return {"success": False, "error": "Таблиця порожня"}

        # Основні обов'язкові заголовки
        mandatory_headers = ["Анкор-1", "Урл-1", "Url"]
        # Усі очікувані заголовки, включаючи опціональні
        all_expected_headers_prefix = ["Анкор-1", "Урл-1", "Анкор-2", "Урл-2", "Анкор-3", "Урл-3", "Url"]
        actual_headers = data[0]

        # Перевіряємо наявність і порядок основних обов'язкових заголовків
        missing_mandatory = [h for h in mandatory_headers if h not in actual_headers]
        if missing_mandatory:
             return {
                "success": False,
                "error": f"Відсутні обов'язкові заголовки: {', '.join(missing_mandatory)}. Очікується щонайменше: {mandatory_headers}",
                "actual_headers": actual_headers
             }

        # Знаходимо індекс 'Url' для перевірки порядку
        try:
             url_index_actual = actual_headers.index("Url")
        except ValueError:
             # Ця помилка вже оброблена вище, але для повноти
             return {"success": False, "error": "Відсутній обов'язковий заголовок 'Url'", "actual_headers": actual_headers}

        # Перевіряємо, чи перші стовпці (до 'Url') відповідають очікуваному префіксу,
        # враховуючи, що Анкор/Урл 2 і 3 можуть бути відсутніми
        expected_prefix_found = True
        current_expected_index = 0
        for i in range(url_index_actual): # Перебираємо стовпці до 'Url'
            # Пропускаємо опціональні заголовки, якщо їх немає в актуальних
            while current_expected_index < len(all_expected_headers_prefix) -1 and \
                  all_expected_headers_prefix[current_expected_index] not in actual_headers:
                 current_expected_index += 2 # Пропускаємо пару Анкор/Урл

            if current_expected_index >= len(all_expected_headers_prefix) -1 or \
               actual_headers[i] != all_expected_headers_prefix[current_expected_index]:
                 expected_prefix_found = False
                 break
            current_expected_index += 1

        if not expected_prefix_found:
             # Створюємо рядок очікуваних заголовків на основі знайдених
             present_expected = [h for h in all_expected_headers_prefix if h in actual_headers]
             return {
                 "success": False,
                 "error": f"Неправильний порядок або назви стовпців перед 'Url'. Очікувались (в такому порядку, якщо присутні): {present_expected[:-1]}, Знайдено: {actual_headers[:url_index_actual]}",
                 "actual_headers": actual_headers
             }

        # Виводимо повідомлення про додаткові стовпці ПІСЛЯ 'Url'
        mandatory_set = set(all_expected_headers_prefix)
        extra_cols = [h for i, h in enumerate(actual_headers) if i > url_index_actual and h not in mandatory_set]
        if extra_cols:
            print(f"Знайдено додаткові стовпці після 'Url': {', '.join(extra_cols)}. Вони будуть проігноровані при обробці.")

        # Перевірка обов'язкових даних (тільки для Анкор-1, Урл-1, Url)
        df = pd.DataFrame(data[1:], columns=actual_headers) # Використовуємо актуальні заголовки
        missing_data = {col: idxs for col in mandatory_headers
                      if col in df.columns and (idxs := [i+2 for i in df[df[col].isin(["", None])].index.tolist()])}

        return {
            "success": not missing_data,
            "error" if missing_data else "message": f"Відсутні дані в обов'язкових стовпцях: {missing_data}" if missing_data else "Таблиця має правильну структуру.",
            "data": data,
            "worksheet": worksheet
        }

    except Exception as e:
        return {"success": False, "error": f"Помилка: {str(e)}"}

def update_sheet_with_results(worksheet, results):
    """Оновлює Google таблицю результатами перевірок URL та посилань."""
    print("\n\n📝 ЗБЕРЕЖЕННЯ РЕЗУЛЬТАТІВ У GOOGLE ТАБЛИЦЮ...\n")

    sheet_data = worksheet.get_all_values()
    headers = sheet_data[0] if sheet_data else []
    if not headers:
        print("⚠️ Помилка: Не вдалося прочитати заголовки з таблиці.")
        return

    # Визначаємо індекс стовпця "Url"
    try:
        url_index = headers.index("Url")
    except ValueError:
        print(f"⚠️ Помилка: Стовпець 'Url' не знайдено в заголовках: {headers}")
        return

    # Базові заголовки результатів (завжди додаються/перевіряються)
    base_result_headers = [
        "Status Code", "Final Redirect URL", "Final Status Code",
        "Robots.txt", "Meta Robots/X-Robots-Tag", "Canonical",
        "Урл-1 наявність", "Анкор-1 співпадає", "Урл-1 rel",
    ]
    
    # Перевіряємо наявність вхідних стовпців для пар 2 і 3
    has_input_pair2 = "Анкор-2" in headers and "Урл-2" in headers
    has_input_pair3 = "Анкор-3" in headers and "Урл-3" in headers
    
    # Формуємо список необхідних заголовків результатів
    required_headers = list(base_result_headers) # Починаємо з базових
    if has_input_pair2:
        required_headers.extend([
            "Урл-2 наявність", "Анкор-2 співпадає", "Урл-2 rel"
        ])
    if has_input_pair3:
         required_headers.extend([
             "Урл-3 наявність", "Анкор-3 співпадає", "Урл-3 rel"
         ])

    new_headers = []
    header_indices = {} # Словник для зберігання індексів ВСІХ потрібних стовпців

    # Заповнюємо індекси існуючих стовпців (включаючи "Url")
    for i, h in enumerate(headers):
        if h in required_headers or h == "Url":
            header_indices[h] = i

    # Додаємо нові заголовки (тільки ті, що потрібні і відсутні) і оновлюємо індекси
    current_col_index = len(headers)
    for header in required_headers:
        if header not in headers:
            new_headers.append(header)
            headers.append(header) # Оновлюємо локальний список заголовків
            header_indices[header] = current_col_index
            current_col_index += 1

    # Оновлюємо заголовки в таблиці, якщо додалися нові
    if new_headers:
        print(f"Додаємо нові заголовки: {', '.join(new_headers)}")
        # Визначаємо діапазон для оновлення заголовків (весь перший рядок)
        header_range = f"A1:{gspread.utils.rowcol_to_a1(1, len(headers))[:-1]}1" # Використовуємо оновлену довжину headers
        worksheet.update(values=[headers], range_name=header_range)
        # Перечитуємо дані, щоб мати актуальну кількість стовпців для подальших оновлень
        sheet_data = worksheet.get_all_values()
        # Перезаповнюємо індекси, оскільки стовпці могли додатись
        header_indices = {}
        for i, h in enumerate(headers): # Використовуємо оновлені headers з таблиці
            if h in required_headers or h == "Url":
                 header_indices[h] = i

    print(f"Збираємо дані для оновлення {len(results)} URL...")

    all_updates = []
    updated_rows = 0
    not_found_urls = []

    # Створюємо словник для швидкого пошуку рядка за URL
    url_to_row_index = {row[url_index]: i + 2 for i, row in enumerate(sheet_data[1:]) if url_index < len(row) and row[url_index]}

    for result in results:
        original_url = result.get("url") # Використовуємо оригінальний URL з результатів
        if not original_url: continue # Пропускаємо, якщо URL не було

        row_idx = url_to_row_index.get(original_url) # Шукаємо індекс рядка

        if row_idx:
            row_updates = {} # Оновлення для поточного рядка [col_index] = value

            # --- Оновлення для базових полів ---
            # (Status Code, Final URL, Final Status, Robots, Meta, Canonical) - ця логіка залишається
            has_redirects = len(result.get("redirect_chain", [])) > 0
            # Status Code / Final Status Code / Final Redirect URL
            if has_redirects:
                if "Status Code" in header_indices: row_updates[header_indices["Status Code"]] = "Redirect"
                if "Final Redirect URL" in header_indices and result.get("final_url") and result["final_url"] != original_url:
                     row_updates[header_indices["Final Redirect URL"]] = result["final_url"]
                else:
                     if "Final Redirect URL" in header_indices: row_updates[header_indices["Final Redirect URL"]] = "" # Очищаємо, якщо URL такий самий
                if "Final Status Code" in header_indices and result.get("final_status_code") is not None:
                     row_updates[header_indices["Final Status Code"]] = str(result["final_status_code"])
            elif "status_code" in result and result.get("status_code") is not None:
                 if "Status Code" in header_indices: row_updates[header_indices["Status Code"]] = str(result["status_code"])
                 # Якщо не було редиректів, очищуємо Final URL та Final Status
                 if header_indices.get("Final Redirect URL"):
                     row_updates[header_indices["Final Redirect URL"]] = ""
                 if header_indices.get("Final Status Code"):
                     row_updates[header_indices["Final Status Code"]] = ""
            elif result.get("error"): # Якщо була помилка запиту (не редирект і не успішний статус)
                if "Status Code" in header_indices: row_updates[header_indices["Status Code"]] = "Error" # Або result["error"]?
                if header_indices.get("Final Redirect URL"): row_updates[header_indices["Final Redirect URL"]] = ""
                if header_indices.get("Final Status Code"): row_updates[header_indices["Final Status Code"]] = ""


            # Robots.txt
            if "Robots.txt" in header_indices:
                 robots_disallowed = []
                 if result.get("robots_star_allowed") is False: robots_disallowed.append("*")
                 if result.get("robots_googlebot_allowed") is False: robots_disallowed.append("Googlebot")
                 row_updates[header_indices["Robots.txt"]] = f"Заборонено ({', '.join(robots_disallowed)})" if robots_disallowed else ""

            # Meta Robots/X-Robots-Tag
            if "Meta Robots/X-Robots-Tag" in header_indices:
                 if dr := result.get("indexing_directives"):
                     tags = []
                     if dr.get("noindex"): tags.append("noindex")
                     if dr.get("nofollow"): tags.append("nofollow")
                     if tags and dr.get("source"):
                         row_updates[header_indices["Meta Robots/X-Robots-Tag"]] = f"{dr['source']}: {', '.join(tags)}"
                     else:
                          row_updates[header_indices["Meta Robots/X-Robots-Tag"]] = "" # Очищаємо, якщо немає тегів або джерела
                 else:
                      row_updates[header_indices["Meta Robots/X-Robots-Tag"]] = "" # Очищаємо, якщо немає директив

            # Canonical
            if "Canonical" in header_indices:
                 if canon_url := result.get("canonical_url"):
                     decoded_canon = unquote(canon_url)
                     target_url_to_compare = result.get("final_url") if has_redirects else normalize_url(original_url)
                     decoded_target = unquote(target_url_to_compare) if target_url_to_compare else ""
                     # Записуємо тільки якщо відрізняється і не порожній
                     row_updates[header_indices["Canonical"]] = canon_url if canon_url and decoded_canon != decoded_target else ""
                 else:
                      row_updates[header_indices["Canonical"]] = "" # Очищаємо, якщо немає

            # --- Оновлення для полів перевірки посилань (з перевірками) ---
            if result.get("final_status_code") == 200: # Записуємо результати посилань тільки якщо була перевірка (статус 200)

                # Пара 1 (завжди перевіряється)
                if "Урл-1 наявність" in header_indices: row_updates[header_indices["Урл-1 наявність"]] = result.get("url1_found", "Ні")
                if "Анкор-1 співпадає" in header_indices: row_updates[header_indices["Анкор-1 співпадає"]] = result.get("anchor1_match", "Ні")
                if "Урл-1 rel" in header_indices:
                    rel_val_1 = result.get("url1_rel")
                    row_updates[header_indices["Урл-1 rel"]] = rel_val_1 if rel_val_1 is not None else ""

                # Пара 2 (тільки якщо відповідні стовпці існують)
                if has_input_pair2 and "Урл-2 наявність" in header_indices:
                    if result.get("Анкор-2") and result.get("Урл-2"): # Чи були дані для перевірки пари 2?
                        row_updates[header_indices["Урл-2 наявність"]] = result.get("url2_found", "Ні")
                        if "Анкор-2 співпадає" in header_indices: row_updates[header_indices["Анкор-2 співпадає"]] = result.get("anchor2_match", "Ні")
                        if "Урл-2 rel" in header_indices:
                             rel_val_2 = result.get("url2_rel")
                             row_updates[header_indices["Урл-2 rel"]] = rel_val_2 if rel_val_2 is not None else ""
                    else: # Якщо даних для пари 2 не було, очищаємо результати (якщо стовпці є)
                        row_updates[header_indices["Урл-2 наявність"]] = ""
                        if "Анкор-2 співпадає" in header_indices: row_updates[header_indices["Анкор-2 співпадає"]] = ""
                        if "Урл-2 rel" in header_indices: row_updates[header_indices["Урл-2 rel"]] = ""

                # Пара 3 (тільки якщо відповідні стовпці існують)
                if has_input_pair3 and "Урл-3 наявність" in header_indices:
                    if result.get("Анкор-3") and result.get("Урл-3"): # Чи були дані для перевірки пари 3?
                        row_updates[header_indices["Урл-3 наявність"]] = result.get("url3_found", "Ні")
                        if "Анкор-3 співпадає" in header_indices: row_updates[header_indices["Анкор-3 співпадає"]] = result.get("anchor3_match", "Ні")
                        if "Урл-3 rel" in header_indices:
                            rel_val_3 = result.get("url3_rel")
                            row_updates[header_indices["Урл-3 rel"]] = rel_val_3 if rel_val_3 is not None else ""
                    else: # Якщо даних для пари 3 не було, очищаємо результати (якщо стовпці є)
                        row_updates[header_indices["Урл-3 наявність"]] = ""
                        if "Анкор-3 співпадає" in header_indices: row_updates[header_indices["Анкор-3 співпадає"]] = ""
                        if "Урл-3 rel" in header_indices: row_updates[header_indices["Урл-3 rel"]] = ""

            else: # Очищаємо всі поля посилань, якщо перевірка не проводилась (статус не 200)
                 # Перевіряємо наявність стовпців перед очищенням
                 if "Урл-1 наявність" in header_indices: row_updates[header_indices["Урл-1 наявність"]] = ""
                 if "Анкор-1 співпадає" in header_indices: row_updates[header_indices["Анкор-1 співпадає"]] = ""
                 if "Урл-1 rel" in header_indices: row_updates[header_indices["Урл-1 rel"]] = ""
                 # Очищення для пари 2, якщо стовпці є
                 if has_input_pair2:
                     if "Урл-2 наявність" in header_indices: row_updates[header_indices["Урл-2 наявність"]] = ""
                     if "Анкор-2 співпадає" in header_indices: row_updates[header_indices["Анкор-2 співпадає"]] = ""
                     if "Урл-2 rel" in header_indices: row_updates[header_indices["Урл-2 rel"]] = ""
                 # Очищення для пари 3, якщо стовпці є
                 if has_input_pair3:
                     if "Урл-3 наявність" in header_indices: row_updates[header_indices["Урл-3 наявність"]] = ""
                     if "Анкор-3 співпадає" in header_indices: row_updates[header_indices["Анкор-3 співпадає"]] = ""
                     if "Урл-3 rel" in header_indices: row_updates[header_indices["Урл-3 rel"]] = ""


            # Додаємо оновлення до масиву, якщо є зміни
            if row_updates:
                current_row_data = sheet_data[row_idx - 1] # row_idx починається з 2, індекс масиву з 0
                update_needed_for_row = False
                for col_idx, value in row_updates.items():
                    # Переконуємося, що індекс існує в словнику (стовпець був створений/знайдений)
                    if col_idx is not None: # header_indices повертає індекс або None якщо немає
                        # Формуємо Cell ID (напр. "A2", "K5")
                        col_letter = gspread.utils.rowcol_to_a1(1, col_idx + 1)[:-1] # +1 бо індекси з 0
                        cell_id = f"{col_letter}{row_idx}"

                        # Порівнюємо нове значення з існуючим (якщо колонка існує в рядку даних)
                        current_value = str(current_row_data[col_idx]) if col_idx < len(current_row_data) else ""
                        new_value = str(value) # Порівнюємо як рядки

                        if new_value != current_value:
                             all_updates.append((cell_id, value))
                             update_needed_for_row = True # Позначаємо, що для цього рядка потрібне оновлення

                if update_needed_for_row:
                     updated_rows += 1
        else:
            not_found_urls.append(original_url)

    if all_updates:
        print(f"Виконується пакетне оновлення {len(all_updates)} комірок...")
        batch_updates_payload = [{'range': cell_id, 'values': [[value]]} for cell_id, value in all_updates]

        # Розбиваємо на частини, якщо оновлень забагато (API може мати ліміти)
        BATCH_SIZE = 500
        for i in range(0, len(batch_updates_payload), BATCH_SIZE):
            batch = batch_updates_payload[i:i + BATCH_SIZE]
            print(f"  Надсилаємо пакет {i//BATCH_SIZE + 1} ({len(batch)} оновлень)...")
            try:
                worksheet.batch_update(batch)
            except gspread.exceptions.APIError as api_e:
                print(f"   ⚠️ Помилка API при оновленні пакету: {api_e}")
                # Можна додати логіку повторної спроби або пропуску
            except Exception as batch_e:
                print(f"   ⚠️ Невідома помилка при оновленні пакету: {batch_e}")


        print(f"Пакетне оновлення завершено!")
    else:
        print("Немає змін для запису в таблицю.")


    print(f"\nРезультати оновлення:")
    print(f"✅ Оновлено рядків (з реальним змінами значень): {updated_rows}")
    if not_found_urls:
        print(f"⚠️ URL, не знайдені в таблиці ({len(not_found_urls)}): {', '.join(not_found_urls[:5])}...")
        if len(not_found_urls) > 5:
            print(f"   ... та ще {len(not_found_urls) - 5}")

#
# 4.5 ФУНКЦІЇ ОБРОБКИ ПОМИЛОК (Google Sheet)
#

def handle_header_error(error, result):
    """Обробляє помилки заголовків."""
    expected = ast.literal_eval(error.split('Очікувалось: ')[1].split(', Отримано:')[0])
    actual = ast.literal_eval(error.split('Отримано: ')[1]) if ', Отримано:' in error else result.get("actual_headers", [])
    print("• Неправильні заголовки стовпців",
          f"\n  Необхідні (по порядку): {', '.join(expected)}",
          f"\n  Знайдено: {', '.join(actual)}",
          "\n• Переконайтеся, що необхідні заголовки розташовані на початку і в правильному порядку")
    # Додаткова інформація про можливі помилки порядку
    if "Неправильний порядок" in error:
         print(f"• Помилка також може бути пов'язана з порядком стовпців перед 'Url'. Деталі: {error.split('. ', 1)[1]}")
    elif "Відсутні обов'язкові заголовки" in error:
         print(f"• {error}")

def handle_missing_data_error(error):
    """Обробляє помилки відсутніх даних."""
    missing_data = ast.literal_eval(error.split("Відсутні дані в обов'язкових стовпцях: ")[1])
    print("• Відсутні дані в обов'язкових стовпцях:")
    [print(f"  - У стовпці '{col}' порожні комірки в рядках: {', '.join(map(str, rows))}")
     for col, rows in missing_data.items()]
    print("• Заповніть всі обов'язкові поля в зазначених рядках")

#
# 5. ФУНКЦІЇ ВІДОБРАЖЕННЯ РЕЗУЛЬТАТІВ
#

def display_sheet_validation_results(result):
    """Виводить результат перевірки у форматі, зрозумілому користувачу."""
    print(f"\n{'='*50}\n🔍 РЕЗУЛЬТАТИ ПЕРЕВІРКИ ТАБЛИЦІ:\n{'='*50}")

    if result["success"]:
        print("✅ УСПІХ! Таблиця має правильну структуру.",
              "\n• Всі необхідні заголовки стовпців розташовані правильно",
              "\n• Всі обов'язкові дані присутні")
        return

    # Обробка помилок - використовуємо словник для диспетчеризації типів помилок
    print("❌ ПОМИЛКА! Виявлено проблеми з таблицею:")
    error = result["error"]

    for err_type, handler in {
        "Неправильний формат URL": lambda: print(f"• {error}\n• Переконайтеся, що ви скопіювали повний URL Google таблиці"),
        "Таблиця порожня": lambda: print(f"• {error}\n• Перевірте, чи є дані в таблиці"),
        "Неправильні заголовки стовпців": lambda: handle_header_error(error, result),
        "Відсутні дані в обов'язкових стовпцях": lambda: handle_missing_data_error(error)
    }.items():
        if err_type in error:
            handler()
            break
    else:
        print(f"• {error}")

    print("="*50) 