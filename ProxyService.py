import threading
import random
from requests import request, get
from datetime import datetime
from pymongo import MongoClient
from .extraction import extract_freeproxy_page


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def proxy_healthy(proxy):
    ping_url = 'http://www.google.com'
    proxies = {
        'http': proxy,
        'https': proxy
    }
    try:
        resp = get(ping_url, timeout=2, proxies=proxies)
        return resp.status_code == 200
    except Exception as e:
        print(str(e))
        return False


class ProxyManager:
    def __init__(self):
        self.db = MongoClient()['scylla']

    def insert_proxies(self, proxies):
        [self.insert_proxy(proxy) for proxy in proxies]

    def insert_proxy(self, proxy):
        self.db.proxies.insert_one(proxy)

    def delete_all_proxies(self):
        self.db.proxies.delete_many({})

    def delete_proxies(self, proxies):
        [self.delete_proxy(proxy) for proxy in proxies]

    def delete_proxy(self, proxy):
        self.db.proxies.delete_one({'ip': proxy['ip']})

    def get_proxies(self, _filter={}):
        return list(self.db.proxies.find(_filter))


class ProxyService:
    def __init__(self):
        self.proxy_manager = ProxyManager()
        self._load_proxies()

    def refresh_proxies(self):
        self.check_proxies(self.proxies, delete=True)
        self.update_proxy_list()

    def update_proxy_list(self):
        _proxies = extract_freeproxy_page()
        if len(_proxies) == 0:
            print('Failed to download proxies.')
            return
        self.check_proxies(_proxies, insert=True)
        self._load_proxies()

    def _load_proxies(self):
        self.proxies = self.proxy_manager.get_proxies()

    def get_proxies(self, nothing=None):
        return self.proxies

    def get_proxies_by_region(self, region):
        key = 'code' if len(region) < 3 else 'country'
        return [proxy for proxy in self.proxies if proxy[key] == region.lower()]

    def get_random_proxy(self, region=None):
        proxy_str = '{}:{}'
        func = self.get_proxies_by_region if region else self.get_proxies
        proxies = func(region)
        proxy = random.choice(proxies)
        return proxy_str.format(proxy['ip'], proxy['port'])

    def check_proxies(self, proxies, insert=False, delete=False, concurrency=20):
        chunk_size = int(len(proxies) / concurrency)
        proxy_groups = list(chunks(proxies, chunk_size))
        for group in proxy_groups:
            threading.Thread(target=self._check_proxies, args=(group, insert, delete, )).start()

    def _check_proxies(self, proxies, insert=False, delete=False):
        for proxy in proxies:
            proxy_str = '{}:{}'
            proxy_address = proxy_str.format(proxy['ip'], proxy['port'])
            if proxy_healthy(proxy_address):
                proxy['last_checked'] = datetime.now()
                if insert:
                    self.proxy_manager.insert_proxy(proxy)
                print('Proxy %s Passed' % proxy_address)
            else:
                if delete:
                    self.proxy_manager.delete_proxy(proxy)
                print('Proxy %s Failed' % proxy_address)


def proxied_request(url, region=None, **kwargs):
    ps = ProxyService()
    url = url.replace('https:', 'http:')
    proxy = ps.get_random_proxy(region)
    kwargs['proxies'] = {
        'http': proxy,
    }
    return request(kwargs.pop('method'), url, **kwargs), proxy
