## МНОГОПОТОЧНЫЙ ПАРСИНГ


import time
import traceback
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)

def get_driver() -> webdriver.Chrome:
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    return webdriver.Chrome(service=service, options=options)

def parse_name(soup: BeautifulSoup) -> str:
    name_block = soup.select_one('h1.product-page__title')
    return name_block.text.strip() if name_block else 'Нет данных'

def parse_price(soup: BeautifulSoup) -> str:
    price_block = soup.select_one('ins.price-block__final-price')
    return price_block.text.strip() if price_block else 'Нет данных'

def parse_description(soup: BeautifulSoup) -> str:
    try:
        desc_block = soup.select_one('section.product-details__description.option p.option__text')
        return desc_block.text.strip() if desc_block else 'Нет данных'
    except Exception as e:
        logging.error(f"Ошибка парсинга описания: {e}")
        return 'Ошибка данных'

def parse_product_params_dict(soup: BeautifulSoup) -> Dict[str, str]:
    params = {}
    try:
        tables = soup.select('div.product-params table.product-params__table')
        for table in tables:
            for row in table.select('tr.product-params__row'):
                label_el = row.select_one('th span span')
                value_el = row.select_one('td span')
                if label_el and value_el:
                    key = label_el.get_text(strip=True)
                    value = value_el.get_text(strip=True)
                    params[key] = value
        for key in ['Высота предмета', 'Глубина предмета', 'Ширина предмета', 'Вес с упаковкой', 'Страна производства']:
            if key not in params:
                row = soup.find('th', string=lambda text: text and key in text)
                if row:
                    td = row.find_next('td')
                    if td:
                        params[key] = td.get_text(strip=True)
    except Exception as e:
        logging.error(f"Ошибка парсинга характеристик как dict: {e}")
    return params

def parse_colors(soup: BeautifulSoup) -> str:
    try:
        colors = [
            block.find('span', class_='color').get('title', 'Без названия').strip()
            for block in soup.find_all('li', class_='color-list__item')
        ]
        return ', '.join(colors) if colors else 'Нет данных'
    except Exception as e:
        logging.error(f"Ошибка парсинга цветов: {e}")
        return 'Ошибка данных'

def parse_sizes(soup: BeautifulSoup) -> str:
    try:
        sizes = [
            block.find('span', class_='size').text.strip()
            for block in soup.find_all('li', class_='j-size')
            if 'disabled' not in block.get('class', [])
        ]
        return ', '.join(sizes) if sizes else 'Нет данных'
    except Exception as e:
        logging.error(f"Ошибка парсинга размеров: {e}")
        return 'Ошибка данных'

def parse_promo(soup: BeautifulSoup) -> str:
    try:
        promos = [
            block.text.strip()
            for block in soup.find_all('div', class_='product-promo__item')
            if block.text.strip()
        ]
        return ' | '.join(promos) if promos else 'Нет акций'
    except Exception as e:
        logging.error(f"Ошибка парсинга акций: {e}")
        return 'Ошибка данных'

def parse_rating(soup: BeautifulSoup) -> str:
    rating_block = soup.select_one('span.product-review__rating')
    return rating_block.text.strip() if rating_block else 'Нет данных'

def parse_reviews(soup: BeautifulSoup) -> str:
    review_block = soup.select_one('span.product-review__count-review')
    return review_block.text.strip() if review_block else '0'

def parse_stock_status(soup: BeautifulSoup) -> str:
    try:
        quantity = soup.select_one('div.qty-block__remaining')
        if quantity:
            return quantity.text.strip()
        buy_button = soup.select_one('button.btn-buy')
        return 'В наличии' if buy_button and 'disabled' not in buy_button.get('class', []) else 'Нет в наличии'
    except Exception as e:
        logging.error(f"Ошибка парсинга наличия: {e}")
        return 'Ошибка данных'


def parse_wildberries_article(article: str) -> Dict[str, str]:
    url = f"https://www.wildberries.ru/catalog/{article}/detail.aspx"
    result: Dict[str, Optional[str]] = {
        'Артикул': article,
        'Ссылка': url,
        'Название': 'Нет данных',  # Инициализируем с "Нет данных"
        'Цена': 'Нет данных',  # Инициализируем с "Нет данных"
        'Описание': 'Нет данных',  # Инициализируем с "Нет данных"
        'Рейтинг': 'Нет данных',  # Инициализируем с "Нет данных"
        'Отзывы': '0',  # Инициализируем с "0"
        'Цвета': 'Нет данных',  # Инициализируем с "Нет данных"
        'Размеры': 'Нет данных',  # Инициализируем с "Нет данных"
        'Акции': 'Нет акций',  # Инициализируем с "Нет акций"
        'Наличие': 'Нет данных',  # Инициализируем с "Нет данных"
    }
    # Добавляем остальные поля характеристик с пустыми значениями по умолчанию
    for key in ['Состав', 'Цвет', 'Пол', 'Сезон', 'Размер на модели', 'Рост модели на фото',
                'Параметры модели на фото (ОГ-ОТ-ОБ)', 'Утеплитель', 'Материал подкладки', 'Тип посадки',
                'Тип карманов', 'Вид застежки', 'Декоративные элементы', 'Особенности модели', 'Уход за вещами',
                'Комплектация', 'Страна производства']:
        result[key] = 'Нет данных'

    driver: Optional[webdriver.Chrome] = None

    try:
        logging.info(f"Артикул {article}: Инициализация WebDriver и открытие URL: {url}")
        driver = get_driver()
        driver.get(url)

        logging.info(f"Артикул {article}: Выполняю начальную прокрутку вниз страницы")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Увеличена задержка после начальной прокрутки
        logging.info(f"Артикул {article}: Начальная прокрутка выполнена")

        # --- Новая проверка на наличие ключевых элементов страницы товара ---
        try:
            logging.info(
                f"Артикул {article}: Проверяю наличие ключевых элементов страницы товара (заголовок или кнопка описания)")
            # Ждем появления либо заголовка, либо кнопки описания
            WebDriverWait(driver, 10).until(  # Короткое ожидание
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, 'h1.product-page__title, button.j-details-btn-desktop'))
            )
            logging.info(f"Артикул {article}: Ключевые элементы страницы товара найдены. Продолжаю парсинг.")

            # --- Существующий блок парсинга описания и характеристик ---
            try:
                button_selector = 'button.j-details-btn-desktop'
                # Проверяем, присутствует ли кнопка описания перед попыткой клика
                button_elements = driver.find_elements(By.CSS_SELECTOR, button_selector)
                if button_elements:
                    logging.info(
                        f"Артикул {article}: Кнопка описания найдена в DOM. Пытаюсь прокрутить ее в видимую область")

                    # Используем первый найденный элемент кнопки
                    button_element = button_elements[0]
                    driver.execute_script("arguments[0].scrollIntoView(true);", button_element)
                    logging.info(
                        f"Артикул {article}: Кнопка прокручена в видимую область. Ожидаю, что она станет кликабельной")

                    button = WebDriverWait(driver, 15).until(  # Увеличено время ожидания кнопки
                        EC.element_to_be_clickable((By.CSS_SELECTOR, button_selector))
                    )
                    logging.info(
                        f"Артикул {article}: Кнопка стала кликабельной. Нажимаю на кнопку 'Характеристики и описание'")
                    button.click()
                    logging.info(f"Артикул {article}: Кнопка 'Характеристики и описание' нажата")

                    logging.info(
                        f"Артикул {article}: Ожидаю видимость блока с описанием (селектор: section.product-details__description.option p.option__text)")
                    WebDriverWait(driver, 20).until(  # Увеличено время ожидания описания
                        EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, 'section.product-details__description.option p.option__text'))
                    )
                    logging.info(f"Артикул {article}: Блок с описанием успешно загружен и виден")
                    time.sleep(2)  # Увеличена задержка после загрузки описания
                else:
                    logging.warning(
                        f"Артикул {article}: Кнопка описания '{button_selector}' не найдена после проверки ключевых элементов.")
                    # Продолжаем парсинг того, что есть на странице без клика на описание

            except Exception as e:
                logging.warning(
                    f"Артикул {article}: Произошла ошибка при попытке клика на описание или ожидании блока описания: {e}")
                # В случае ошибки при работе с описанием, продолжаем парсить остальное
                pass
            # --- Конец блока парсинга описания и характеристик ---

            logging.info(f"Артикул {article}: Получаю исходный код страницы для парсинга Beautiful Soup")
            soup = BeautifulSoup(driver.page_source, 'lxml')
            logging.info(f"Артикул {article}: Beautiful Soup инициализирован")

            if soup:
                logging.info(f"Артикул {article}: Начинаю парсинг данных с помощью Beautiful Soup")
                result['Название'] = parse_name(soup)
                result['Цена'] = parse_price(soup)
                result['Описание'] = parse_description(soup)
                result.update(parse_product_params_dict(soup))
                result['Рейтинг'] = parse_rating(soup)
                result['Отзывы'] = parse_reviews(soup)
                result['Цвета'] = parse_colors(soup)
                result['Размеры'] = parse_sizes(soup)
                result['Акции'] = parse_promo(soup)
                result['Наличие'] = parse_stock_status(soup)
                logging.info(f"Артикул {article}: Парсинг данных завершен")

        except Exception as e:
            logging.warning(
                f"Артикул {article}: Не удалось найти ключевые элементы страницы товара в течение таймаута. Возможно, загрузилась страница проверки или страница ошибки. Ошибка: {e}")
            # Если ключевые элементы не найдены, результат останется с дефолтными "Нет данных"
            pass
        # --- Конец новой проверки ---


    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logging.error(f"Артикул {article}: Критическая ошибка при парсинге: {error_msg}")
        traceback.print_exc()

    finally:
        if driver:
            logging.info(f"Артикул {article}: Закрываю WebDriver")
            driver.quit()
            logging.info(f"Артикул {article}: WebDriver закрыт")

    return result

def main():
    input_file = 'articles.xlsx'
    max_workers = 2 ## MAX количество потоков

    try:
        df = pd.read_excel(input_file)
        if 'Артикул' not in df.columns:
            raise ValueError("Входной файл не содержит колонку 'Артикул'")
        articles = df['Артикул'].dropna().astype(str).tolist()
        logging.info(f"Найдено {len(articles)} артикулов для обработки")
    except Exception as e:
        logging.error(f"Ошибка чтения файла: {e}")
        return

    results: List[Dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_article = {executor.submit(parse_wildberries_article, art): art for art in articles}
        for idx, future in enumerate(as_completed(future_to_article), 1):
            article = future_to_article[future]
            try:
                data = future.result()
                if 'Название' in data:
                    results.append(data)
                    logging.info(f"[{idx}/{len(articles)}] Обработан артикул: {article} - {data['Название'][:30]}")
                else:
                    logging.warning(f"[{idx}/{len(articles)}] Пропущен артикул: {article} — данные не получены")
            except Exception as e:
                logging.error(f"Ошибка в потоке для артикула {article}: {e}")

    today_str = datetime.now().strftime('%Y-%m-%d_%H-%M')
    output_file = f'wildberries_data_{today_str}.xlsx'

    df_result = pd.json_normalize(results)
    df_result.to_excel(output_file, index=False)
    logging.info(f"Результаты сохранены в файл: {output_file}")
    logging.info(f"Успешно обработано: {len(results)}/{len(results)}")

if __name__ == '__main__':
    main()

