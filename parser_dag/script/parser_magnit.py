import aiohttp
import asyncio
import requests
from bs4 import BeautifulSoup
from aiohttp_retry import RetryClient, ExponentialRetry
from fake_useragent import UserAgent
import csv
import random, pandas
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector
import datetime
import pandas as pd
class ParserProducts:

    def __init__(self, url):
        self.url = url
        self.headers = {'User-Agent': UserAgent().random}
        self.products = []
        self.trademark = ['кофейня на паяхъ', 'Мясницкий ряд', 'Ремит', 'Мираторг',
                          'Простоквашино', 'Домик в деревне', 'Село Зеленое', 'Красный Октябрь',
                          'РотФронт', 'Яшкино', 'Хлебный Дом', 'Брест-Литовск', 'Добрый'
                          ]

    @staticmethod
    def get_soup(url):
        print(url)
        session = requests.Session()
        resp = session.get(url=url, headers={'User-Agent': UserAgent().random})
        return BeautifulSoup(resp.text, 'lxml')

    def get_pages(self, soup):
        return int(soup.find_all('span', class_="pl-button__icon")[-2].text)

    async def pars(self, link, session):
        retry_options = ExponentialRetry(attempts=5)
        retry_client = RetryClient(raise_for_status=False, retry_options=retry_options, client_session=session,
                                   start_timeout=0.5)
        async with retry_client.get(link) as response:
            if response:
                soup = BeautifulSoup(await response.text(), 'lxml')
                for tag in soup.find_all('div',
                                         class_="pl-stack-item pl-stack-item_size-6 pl-stack-item_size-4-m pl-stack-item_size-3-ml unit-catalog__stack-item"):
                    if tag.find('a'):
                        for firm in self.trademark:
                            if self.get_trademark(firm, tag.find('a')['title']):
                                async with retry_client.get('https://magnit.ru' + tag.find('a')['href']) as resp:
                                    s = BeautifulSoup(await resp.text(), 'lxml')
                                    if s.find('span', class_='pl-text product-details-offer__title'):
                                        l = []
                                        for tags in s.find_all('div',
                                                               class_='product-details-parameters-list__item'):
                                            t = tags.find_all('span', class_='pl-text')
                                            l.append(t[0].text + ' ' + t[1].text)
                                        l.append('price' + ' ' + s.find('span', class_='pl-text product-details-price__current').text)
                                        l.append('date_price' + ' ' + str(pd.to_datetime('today').normalize()))
                                        self.products.append(l)

    @staticmethod
    def get_trademark(firm, title):
        if firm in title:
            return True
        return False

    async def main(self):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for i in range(1, self.get_pages(self.get_soup(self.url)) + 1):
                task = asyncio.create_task(self.pars(f'{self.url}&page={i}', session))
                tasks.append(task)
            await asyncio.gather(*tasks)

    def __call__(self, *args, **kwargs):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(self.main())





