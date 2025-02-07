import json
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (ElementNotInteractableException, ElementClickInterceptedException,
                                        NoSuchElementException)
import csv
import random
from selenium import webdriver
from seleniumwire import webdriver
import pandas as pd


class ParserDixi:
    def __init__(self, url, prod):
        self.headers = {'User-Agent': UserAgent().random}
        self.data = []
        self.prod = prod
        self.url = url
        self.link = []
        self.prod_inf = []
        self.proxy = random.choice(
            ['socks5://hZdSqH:0SCocw@188.130.203.29:8000', 'socks5://cLaFFB:JgGmZC@196.18.12.18:8000',
             'socks5://cLaFFB:JgGmZC@196.18.15.117:8000'])
        self.trademark = ['кофейня на паяхъ', 'Мясницкий ряд', 'Ремит', 'Мираторг',
                          'Простоквашино', 'Домик в деревне', 'Село Зеленое', 'Красный Октябрь',
                          'РотФронт', 'Яшкино', 'Хлебный Дом', 'Брест-Литовск', 'Добрый'
                          ]

    @staticmethod
    def get_trademark(firm, title):
        if firm in title:
            return True
        return False

    def pars(self, link):
        proxi_options = {
            "proxy": {
                'http': self.proxy,
                'https': self.proxy
            },
        }
        options = webdriver.ChromeOptions()
        options.page_load_strategy = 'eager'
        service = Service(executable_path=ChromeDriverManager().install())
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(service=service, options=options, seleniumwire_options=proxi_options)
        driver.implicitly_wait(5)
        with driver as browser:
            browser.get(link)
            time.sleep(15)
            with open('cookies_dixi.json', 'r') as file:
                cookies = json.load(file)
                for cookie in cookies:
                    browser.add_cookie(cookie)
            browser.refresh()
            browser.maximize_window()
            while True:
                try:
                    browser.find_element(By.XPATH, "//button[@class='btn grey more-orders']").click()
                except (ElementNotInteractableException, NoSuchElementException):
                    break
            html = browser.page_source
            if html:
                soup = BeautifulSoup(html, 'lxml')
                for t in soup.find_all('article', class_='card bs-state'):
                    if t.find('a'):
                        try:
                            for firm in self.trademark:
                                if self.get_trademark(firm, t.find('h3').text):
                                    self.data.append(t.find('a')['href'])
                                    print(t.find('a')['href'])
                        except AttributeError:
                            pass

    def get_link(self):
        x = -1
        while x < len(self.prod) - 1:
            x += 1
            try:
                print(self.url + self.prod[x])
                self.pars(self.url + self.prod[x])
                self.link += self.data
                print(x)
                print(len(self.link))
            except ElementClickInterceptedException:
                x -= 1
                print(len(self.link))

    def get_prod(self):
        dict_name_column = {'Тип товара': 'Тип продукта',  'Торговая марка': 'Бренд'}
        self.get_link()
        session = requests.Session()
        for l in self.link:
            with session.get(self.url + l[9:], headers=self.headers,
                             proxies={'https': self.proxy}) as responce:
                if responce:
                    soup = BeautifulSoup(responce.text, 'lxml')
                    title = soup.find('h1').text
                    product_information = soup.find_all('div', class_='list__line')
                    l = []
                    price = soup.find('div', class_='card__price-num')
                    for i in product_information:
                        if i.find('span', class_='text').text in (
                                'Тип товара', 'Торговая марка'):
                            if i.find('a'):
                                l.append(
                                    dict_name_column[i.find('span', class_='text').text] + ' ' + i.find('a').find('span').text)
                            else:
                                l.append(i.find('span', class_='text').text + ' ' + 'None')
                    l.append('price' + ' ' + price.text)
                    l.append('date_price' + ' ' + str(pd.to_datetime('today').normalize()))
                    self.prod_inf.append(l)


prod = ['molochnye-produkty-yaytsa/', 'konditerskie-izdeliya-torty/', 'ovoshchi-frukty/', 'khleb-i-vypechka/', 'myaso-ptitsa/', 'myasnaya-gastronomiya/', 'chay-kofe-kakao/']
url = 'https://dixy.ru/catalog/'

data = []

for p in prod:
    PD = ParserDixi(url, [p])
    PD.get_prod()
    data += PD.prod_inf
with open('products.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    for row in data:  # запись строк
        writer.writerow(row)


