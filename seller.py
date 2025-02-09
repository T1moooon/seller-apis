import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров магазина на платформе Ozon.

    Args:
        last_id (str): Идентификатор последнего товара в предыдущем запросе.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен доступа для авторизации в API Ozon.

    Returns:
        dict: Словарь, содержащий информацию о товарах.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API Ozon завершился с ошибкой.

    Examples:
        >>> get_product_list(last_id, client_id, seller_token)
        {
            "result": {
                "items": [
                    {
                        "product_id": 223681945,
                        "offer_id": "136748"
                    }
                ],
                "total": 1,
                "last_id": "bnVсbA=="
            }
        }
 
         >>> get_product_list("invalid_id", client_id, seller_token)
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает список артикулов товаров магазина на платформе Ozon.

    Args:
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен доступа для авторизации в API Ozon.

    Returns:
        list: Список артикулов товаров.

    Examples:
        >>> get_offer_ids(client_id, seller_token)
        ["123", "456", "789"]

        >>> get_offer_ids("invalid_client", "invalid_token")
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров на платформе Ozon.

    Args:
        prices (list): Список цен для обновления.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен доступа для авторизации в API Ozon.

    Returns:
        dict: Словарь с результатами обновления цен.

    Examples:
        >>> update_price(price, client_id, seller_token)
        {
            "result": [
                {
                    "product_id": 1386,
                    "offer_id": "PH8865",
                    "updated": true,
                    "errors": [ ]
                }
            ]
        }

        >>> update_price([], client_id, seller_token)
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет информацию о количестве товаров на складе на платформе Ozon.
    
    Args:
        stocks (list): Список с информацией о товарах на складе.
        client_id (str): Идентификатор клиента на платформе Ozon.
        seller_token (str): Токен доступа для авторизации в API Ozon.

    Returns:
        dict: Словарь с результатами обновления остатков.

    Examples:
        >>> update_stocks(stocks, client_id, seller_token)
        {
            "result": {
                "items": [
                {
                    "product_id": 214887921,
                    "offer_id": "136834",
                    "stocks": [
                    {
                        "type": "fbs",
                        "present": 170,
                        "reserved": 0
                    },
                    {
                        "type": "fbo",
                        "present": 0,
                        "reserved": 0
                    },
                    {
                        "type": "crossborder",
                        "present": 170,
                        "reserved": 0
                    }
                    ]
                }
                ],
                "total": 1,
                "last_id": "anVsbA=="
            }
        }
        
        >>> update_stocks([], "client123", "seller_token_abc")
        requests.exceptions.HTTPError: 400 Client Error: Bad Request
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает файл с остатками товаров с сайта поставщика и возвращает данные.

    Returns:
        list: Список словарей с информацией о товарах.

    Examples:
        >>> download_stock()
        [
            {"Код": "123", "Количество": "10", "Цена": "1000 руб."},
            {"Код": "456", "Количество": "5", "Цена": "2000 руб."}
        ]

        >>> download_stock()
        requests.exceptions.HTTPError: 404 Client Error: Not Found
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает список с информацией о количестве товаров на складе.

    Args:
        watch_remnants (list): Список с данными о товарах.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список словарей с информацией о количестве товаров.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids)
        [{"offer_id": "123", "stock": 10}]

        >>> create_stocks([], offer_ids)
        [{"offer_id": "123", "stock": 0}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает список с информацией о ценах на товары.

    Args:
        watch_remnants (list): Список с данными о товарах.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список словарей с информацией о ценах.

    Examples:
        >>> create_prices(watch_remnants, offer_ids)
        [{"offer_id": "123", "price": "1000"}]

        >>> create_prices([], offer_ids)
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует строку с ценой в числовой формат, удаляя лишние символы.

    Args:
        price (str): Строка с ценой.

    Returns:
        str: Строка, в которой будет числовое значение цены.

    Raises:
        TypeError: Если входная строка не содержит числовых символов или имеет некорректный формат.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        >>> price_conversion("abc")
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на части по указанному количеству элементов.

    Args:
        lst (list): Список для разделения.
        n (int): Количество элементов в каждой части.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        >>> list(divide([], 2))
        []
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
