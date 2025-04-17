import requests
from urllib.parse import urljoin, unquote
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup

from utils import normalize_text, normalize_url

#
# 2. ФУНКЦІЇ SEO-ПЕРЕВІРОК
#

def check_robots_txt(url_to_check, user_agent='*', verify_ssl=True):
    """Перевіряє доступність URL в robots.txt для вказаного user-agent."""
    print(f"   ├── Перевірка robots.txt для User-agent: {user_agent}...")
    normalized_url = normalize_url(url_to_check) # Нормалізуємо перед перевіркою
    robots_url = urljoin(normalized_url, '/robots.txt')
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        # Використовуємо стандартний User-Agent для запиту robots.txt
        with requests.get(robots_url, timeout=5, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.184 Safari/537.36'}, verify=verify_ssl) as resp:
            if resp.status_code == 200:
                rp.parse(resp.text.splitlines())
                is_allowed = rp.can_fetch(user_agent, normalized_url)
                print(f"   │   └── {'✅ Дозволено' if is_allowed else '❌ Заборонено'} в robots.txt для {user_agent}")
                return is_allowed
            elif resp.status_code == 404:
                print(f"   │   └── ✅ robots.txt не знайдено (404), сканування дозволено")
                return True # Якщо robots.txt немає, сканування дозволено
            else:
                print(f"   │   └── ⚠️ Не вдалося отримати robots.txt (Статус: {resp.status_code}), припускаємо, що дозволено")
                return True # В разі помилки краще вважати, що дозволено
    except requests.exceptions.RequestException as e:
        print(f"   │   └── ⚠️ Помилка при запиті до robots.txt: {e}, припускаємо, що дозволено")
        return True

def check_indexing_directives(url, headers, html_content):
    """Перевіряє наявність noindex/nofollow в X-Robots-Tag та мета-тегах."""
    print(f"   ├── Перевірка директив індексації (X-Robots-Tag/Meta Robots)...")
    directives = {'noindex': False, 'nofollow': False, 'source': None}

    # 1. Перевірка X-Robots-Tag в заголовках
    x_robots_tag = headers.get('X-Robots-Tag', headers.get('x-robots-tag'))
    if x_robots_tag:
        print(f"   │   ├── Знайдено X-Robots-Tag: {x_robots_tag}")
        content = x_robots_tag.lower()
        if 'noindex' in content:
            directives['noindex'] = True
            directives['source'] = 'X-Robots-Tag'
        if 'nofollow' in content:
            directives['nofollow'] = True
            directives['source'] = directives.get('source', 'X-Robots-Tag') # Keep source if already set

    # 2. Якщо в заголовках немає, перевірка мета-тегів в HTML
    if not directives['source']:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            # Пріоритет для Googlebot, потім загальний robots
            meta_tag_google = soup.find('meta', attrs={'name': 'googlebot'})
            meta_tag_robots = soup.find('meta', attrs={'name': 'robots'})

            meta_tag = meta_tag_google or meta_tag_robots # Використовуємо тег для Googlebot якщо є

            if meta_tag and meta_tag.get('content'):
                tag_name = meta_tag.get('name', 'robots').capitalize()
                content = meta_tag['content'].lower()
                print(f"   │   ├── Знайдено Meta {tag_name}: {meta_tag['content']}")
                if 'noindex' in content:
                    directives['noindex'] = True
                    directives['source'] = f'Meta {tag_name}'
                if 'nofollow' in content:
                    directives['nofollow'] = True
                    directives['source'] = directives.get('source', f'Meta {tag_name}')
            else:
                 print(f"   │   └── Директиви в мета-тегах не знайдено.")
        except Exception as e:
             print(f"   │   └── ⚠️ Помилка парсингу HTML для мета-тегів: {e}")

    if not directives['source']:
         print(f"   │   └── Директиви індексації (noindex/nofollow) не знайдено.")
    else:
        ni_status = '❌ NOINDEX' if directives['noindex'] else '✅ index'
        nf_status = '❌ NOFOLLOW' if directives['nofollow'] else '✅ follow'
        print(f"   │   └── Результат: {ni_status}, {nf_status} (Джерело: {directives['source']})")

    return directives

def check_canonical_tag(url, html_content):
    """Перевіряє наявність canonical тега і порівнює з поточним URL."""
    print(f"   ├── Перевірка Canonical тега...")
    normalized_current_url = normalize_url(url) # Нормалізуємо поточний URL для порівняння
    canonical_url = None
    source_canonical = None
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        link_tag = soup.find('link', rel='canonical')
        if link_tag and link_tag.get('href'):
            # Робимо URL абсолютним і нормалізуємо
            source_canonical = link_tag['href']
            canonical_url = normalize_url(urljoin(normalized_current_url, source_canonical))

            print(f"   │   ├── Знайдено Canonical: {canonical_url}")
            # Порівнюємо декодовані версії URL
            if unquote(canonical_url) == unquote(normalized_current_url):
                print(f"   │   └── ✅ Canonical співпадає з поточним URL.")
            else:
                print(f"   │   └── ⚠️ Canonical відрізняється від поточного URL ({normalized_current_url}).")
        else:
             print(f"   │   └── Canonical тег не знайдено.")
    except Exception as e:
        print(f"   │   └── ⚠️ Помилка парсингу HTML для Canonical: {e}")
    return canonical_url

# --- ОНОВЛЕНА ФУНКЦІЯ ---
def check_links_on_page(html_content, page_url, anchor1, url1, anchor2, url2, anchor3, url3):
    """Шукає вказані пари URL+Анкор на сторінці, пріоритезуючи точні співпадіння."""
    print(f"   ├── Перевірка наявності посилань та анкорів на {page_url}...")
    results = {
        "url1_found": "Ні", "anchor1_match": "Ні", "url1_rel": None,
        "url2_found": "Ні", "anchor2_match": "Ні", "url2_rel": None,
        "url3_found": "Ні", "anchor3_match": "Ні", "url3_rel": None,
        "error": None
    }

    normalized_url1 = normalize_url(url1) if url1 else None
    normalized_anchor1 = normalize_text(anchor1) if anchor1 else None
    normalized_url2 = normalize_url(url2) if url2 else None
    normalized_anchor2 = normalize_text(anchor2) if anchor2 else None
    normalized_url3 = normalize_url(url3) if url3 else None
    normalized_anchor3 = normalize_text(anchor3) if anchor3 else None

    rel_attrs_to_check = {"nofollow", "sponsored", "noindex"}

    # --- Трекери стану ---
    pair1_exact_match_found = False
    pair2_exact_match_found = False
    pair3_exact_match_found = False
    link_indices_used_by_exact_matches = set() # Зберігаємо індекси посилань, що точно співпали

    # Інформація про перше знайдене посилання з правильним URL, але неправильним анкором
    url1_mismatch_info = None # {'url': url, 'found_anchor': anchor, 'rel': rel, 'text': text, 'index': index}
    url2_mismatch_info = None # {'url': url, 'found_anchor': anchor, 'rel': rel, 'text': text, 'index': index}
    url3_mismatch_info = None # {'url': url, 'found_anchor': anchor, 'rel': rel, 'text': text, 'index': index}

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = soup.find_all('a', href=True)

        for index, link in enumerate(links):
            href = link.get('href')
            try:
                # Робимо URL абсолютним та нормалізуємо його
                absolute_href = urljoin(page_url, href)
                normalized_found_url = normalize_url(absolute_href)
            except Exception:
                continue # Пропускаємо невалідні URL

            link_text = link.get_text(strip=True)
            normalized_found_anchor = normalize_text(link_text)
            # Отримуємо значення rel як множину і перевіряємо цікаві для нас
            rel_values = set(link.get('rel', []))
            found_rel_str = ", ".join(sorted(list(rel_values.intersection(rel_attrs_to_check)))) or None

            # --- Перевірка для Пари 1 ---
            if not pair1_exact_match_found and normalized_url1 and normalized_found_url == normalized_url1:
                if normalized_anchor1 and normalized_found_anchor == normalized_anchor1:
                    # Знайдено точне співпадіння для Пари 1
                    pair1_exact_match_found = True
                    link_indices_used_by_exact_matches.add(index) # Запам'ятовуємо індекс
                    results["url1_found"] = "Так"
                    results["anchor1_match"] = "Так"
                    results["url1_rel"] = found_rel_str
                    # Виводимо повідомлення про успіх для Пари 1
                    print(f"   │   ├── ✅ Знайдено Урл-1: {absolute_href}")
                    print(f"   │   │   └── Текст посилання: '{link_text}'")
                    print(f"   │   │   └── ✅ Анкор-1 співпадає (Нормалізовано: '{normalized_found_anchor}')")
                    if found_rel_str:
                        print(f"   │   │   └── ⚠️ Знайдено атрибути rel для Урл-1: {found_rel_str}")
                    else:
                        print(f"   │   │   └── ✅ Атрибути 'rel' ({', '.join(rel_attrs_to_check)}) для Урл-1 не знайдено.")
                elif url1_mismatch_info is None and normalized_anchor1: # Перевіряємо mismatch тільки якщо anchor1 задано
                    # Знайдено першу невідповідність для Пари 1 (URL ОК, Анкор не той)
                    url1_mismatch_info = {
                        'url': absolute_href,
                        'found_anchor': link_text,
                        'found_anchor_normalized': normalized_found_anchor,
                        'rel': found_rel_str,
                        'text': link_text,
                        'index': index
                    }

            # --- Перевірка для Пари 2 ---
            # Переконуємося, що це посилання ще не було використане для точного співпадіння іншої пари
            if not pair2_exact_match_found and normalized_url2 and normalized_found_url == normalized_url2 and index not in link_indices_used_by_exact_matches:
                 if normalized_anchor2 and normalized_found_anchor == normalized_anchor2:
                    # Знайдено точне співпадіння для Пари 2
                    pair2_exact_match_found = True
                    link_indices_used_by_exact_matches.add(index) # Запам'ятовуємо індекс
                    results["url2_found"] = "Так"
                    results["anchor2_match"] = "Так"
                    results["url2_rel"] = found_rel_str
                    # Виводимо повідомлення про успіх для Пари 2
                    print(f"   │   ├── ✅ Знайдено Урл-2: {absolute_href}")
                    print(f"   │   │   └── Текст посилання: '{link_text}'")
                    print(f"   │   │   └── ✅ Анкор-2 співпадає (Нормалізовано: '{normalized_found_anchor}')")
                    if found_rel_str:
                        print(f"   │   │   └── ⚠️ Знайдено атрибути rel для Урл-2: {found_rel_str}")
                    else:
                        print(f"   │   │   └── ✅ Атрибути 'rel' ({', '.join(rel_attrs_to_check)}) для Урл-2 не знайдено.")
                 elif url2_mismatch_info is None and normalized_anchor2: # Перевіряємо mismatch тільки якщо anchor2 задано
                     # Знайдено першу невідповідність для Пари 2 (URL ОК, Анкор не той)
                     url2_mismatch_info = {
                         'url': absolute_href,
                         'found_anchor': link_text,
                         'found_anchor_normalized': normalized_found_anchor,
                         'rel': found_rel_str,
                         'text': link_text,
                         'index': index
                     }

            # --- Перевірка для Пари 3 ---
            # Переконуємося, що це посилання ще не було використане для точного співпадіння іншої пари
            if not pair3_exact_match_found and normalized_url3 and normalized_found_url == normalized_url3 and index not in link_indices_used_by_exact_matches:
                 if normalized_anchor3 and normalized_found_anchor == normalized_anchor3:
                    # Знайдено точне співпадіння для Пари 3
                    pair3_exact_match_found = True
                    link_indices_used_by_exact_matches.add(index) # Запам'ятовуємо індекс
                    results["url3_found"] = "Так"
                    results["anchor3_match"] = "Так"
                    results["url3_rel"] = found_rel_str
                    # Виводимо повідомлення про успіх для Пари 3
                    print(f"   │   ├── ✅ Знайдено Урл-3: {absolute_href}")
                    print(f"   │   │   └── Текст посилання: '{link_text}'")
                    print(f"   │   │   └── ✅ Анкор-3 співпадає (Нормалізовано: '{normalized_found_anchor}')")
                    if found_rel_str:
                        print(f"   │   │   └── ⚠️ Знайдено атрибути rel для Урл-3: {found_rel_str}")
                    else:
                        print(f"   │   │   └── ✅ Атрибути 'rel' ({', '.join(rel_attrs_to_check)}) для Урл-3 не знайдено.")
                 elif url3_mismatch_info is None and normalized_anchor3: # Перевіряємо mismatch тільки якщо anchor3 задано
                     # Знайдено першу невідповідність для Пари 3 (URL ОК, Анкор не той)
                     url3_mismatch_info = {
                         'url': absolute_href,
                         'found_anchor': link_text,
                         'found_anchor_normalized': normalized_found_anchor,
                         'rel': found_rel_str,
                         'text': link_text,
                         'index': index
                     }


        # --- Обробка результатів та вивід повідомлень ПІСЛЯ циклу ---

        # Пара 1: Якщо точного співпадіння не було, перевіряємо невідповідність або повну відсутність
        if not pair1_exact_match_found:
            # Переконуємося, що можливий mismatch для url1 не використовував те ж посилання, що й точні збіги для інших пар
            can_use_mismatch1 = url1_mismatch_info and url1_mismatch_info['index'] not in link_indices_used_by_exact_matches

            if can_use_mismatch1:
                # Виводимо інформацію про невідповідність (Формат 1.А)
                print(f"   │   ├── ⚠️ Знайдено Урл-1: {url1_mismatch_info['url']}")
                print(f"   │   │   └── Текст посилання: '{url1_mismatch_info['text']}'")
                print(f"   │   │   └── ❌ Анкор-1 не співпадає (Очікувався: '{normalized_anchor1}', Знайдено: '{url1_mismatch_info['found_anchor']}')")
                mismatch_rel = url1_mismatch_info['rel']
                if mismatch_rel:
                     print(f"   │   │   └── Атрибути 'rel' для знайденого посилання: {mismatch_rel}")
                else:
                     print(f"   │   │   └── Атрибути 'rel' для знайденого посилання: Не знайдено")
                # Оновлюємо результати: URL знайдено, але анкор не той
                results["url1_found"] = "Так"
                results["anchor1_match"] = "Ні"
                results["url1_rel"] = mismatch_rel # Зберігаємо rel з невідповідного посилання
            elif normalized_url1: # Виводимо "не знайдено" тільки якщо ми шукали цей URL
                print(f"   │   └── ❌ Точну пару Урл-1/Анкор-1 ({url1} / '{anchor1}') не знайдено.")

        # Пара 2: Якщо точного співпадіння не було, перевіряємо невідповідність або повну відсутність
        if not pair2_exact_match_found:
            # Переконуємося, що можливий mismatch для url2 не використовував те ж посилання, що й точні збіги для інших пар
            can_use_mismatch2 = url2_mismatch_info and url2_mismatch_info['index'] not in link_indices_used_by_exact_matches

            if can_use_mismatch2:
                 # Виводимо інформацію про невідповідність (Формат 1.А)
                 print(f"   │   ├── ⚠️ Знайдено Урл-2: {url2_mismatch_info['url']}")
                 print(f"   │   │   └── Текст посилання: '{url2_mismatch_info['text']}'")
                 print(f"   │   │   └── ❌ Анкор-2 не співпадає (Очікувався: '{normalized_anchor2}', Знайдено: '{url2_mismatch_info['found_anchor']}')")
                 mismatch_rel = url2_mismatch_info['rel']
                 if mismatch_rel:
                      print(f"   │   │   └── Атрибути 'rel' для знайденого посилання: {mismatch_rel}")
                 else:
                      print(f"   │   │   └── Атрибути 'rel' для знайденого посилання: Не знайдено")
                 # Оновлюємо результати
                 results["url2_found"] = "Так"
                 results["anchor2_match"] = "Ні"
                 results["url2_rel"] = mismatch_rel
            elif normalized_url2: # Виводимо "не знайдено" тільки якщо ми шукали цей URL
                 print(f"   │   └── ❌ Точну пару Урл-2/Анкор-2 ({url2} / '{anchor2}') не знайдено.")

        # Пара 3: Якщо точного співпадіння не було, перевіряємо невідповідність або повну відсутність
        if not pair3_exact_match_found:
            # Переконуємося, що можливий mismatch для url3 не використовував те ж посилання, що й точні збіги для інших пар
            can_use_mismatch3 = url3_mismatch_info and url3_mismatch_info['index'] not in link_indices_used_by_exact_matches

            if can_use_mismatch3:
                 # Виводимо інформацію про невідповідність
                 print(f"   │   ├── ⚠️ Знайдено Урл-3: {url3_mismatch_info['url']}")
                 print(f"   │   │   └── Текст посилання: '{url3_mismatch_info['text']}'")
                 print(f"   │   │   └── ❌ Анкор-3 не співпадає (Очікувався: '{normalized_anchor3}', Знайдено: '{url3_mismatch_info['found_anchor']}')")
                 mismatch_rel = url3_mismatch_info['rel']
                 if mismatch_rel:
                      print(f"   │   │   └── Атрибути 'rel' для знайденого посилання: {mismatch_rel}")
                 else:
                      print(f"   │   │   └── Атрибути 'rel' для знайденого посилання: Не знайдено")
                 # Оновлюємо результати
                 results["url3_found"] = "Так"
                 results["anchor3_match"] = "Ні"
                 results["url3_rel"] = mismatch_rel
            elif normalized_url3: # Виводимо "не знайдено" тільки якщо ми шукали цей URL
                 print(f"   │   └── ❌ Точну пару Урл-3/Анкор-3 ({url3} / '{anchor3}') не знайдено.")


    except Exception as e:
        error_message = f"Помилка парсингу HTML для пошуку посилань: {e}"
        print(f"   │   └── ⚠️ {error_message}")
        results["error"] = error_message # Записуємо помилку в результати

    # Якщо не було помилки парсингу, перевіряємо, чи взагалі шукали щось
    if not results["error"]:
        # Додаємо перевірку для url3
        if not normalized_url1 and not normalized_url2 and not normalized_url3:
             print(f"   │   └── Не вказано Урл-1, Урл-2 та Урл-3 для пошуку.")
        elif not pair1_exact_match_found and not url1_mismatch_info and not normalized_url1: # Якщо не шукали Урл-1
             pass # Нічого не виводити для пари 1
        elif not pair2_exact_match_found and not url2_mismatch_info and not normalized_url2: # Якщо не шукали Урл-2
             pass # Нічого не виводити для пари 2
        elif not pair3_exact_match_found and not url3_mismatch_info and not normalized_url3: # Якщо не шукали Урл-3
             pass # Нічого не виводити для пари 3

    return results
# --- КІНЕЦЬ ОНОВЛЕНОЇ ФУНКЦІЇ --- 