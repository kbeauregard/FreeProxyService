import threading
import random
from time import sleep
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
        resp = get(ping_url, timeout=3, proxies=proxies)
        resp2 = get(ping_url, timeout=3, proxies=proxies)
        return resp.status_code == 200 or resp2.status_code == 200
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

    def get_proxy(self, proxy):
        return self.db.proxies.find_one({'ip': proxy['ip']})

    def jail_proxy(self, proxy):
        self.db.proxies.update_one({'ip': proxy['ip']}, {'$set': {'jailed': True, 'last_jailed': datetime.now()}})

    def unjail_proxy(self, proxy):
        self.db.proxies.update_one({'ip': proxy['ip']}, {'$set': {'jailed': False, 'last_unjailed': datetime.now()}})


class ProxyService:
    def __init__(self):
        self.proxy_manager = ProxyManager()

    def refresh_proxies(self):
        self.check_proxies(jail=True)
        self.update_proxy_list()

    def update_proxy_list(self):
        _proxies = extract_freeproxy_page()
        if len(_proxies) == 0:
            print('Failed to download proxies.')
            return
        self.check_proxies(_proxies, insert=True)

    def get_proxies(self, _filter={}):
        _filter['jailed'] = False
        return self.proxy_manager.get_proxies(_filter)

    def get_proxies_by_region(self, region):
        key = 'code' if len(region) < 3 else 'country'
        return self.proxy_manager.get_proxies({key: region.lower(), 'jailed': False})

    def get_random_proxy(self, region={}):
        # No idea why but it doesn't work without this, somehow region = {'jailed': False} when called with no args
        if 'jailed' in region:
            region.pop('jailed')
        proxy_str = '{}:{}'
        func = self.get_proxies_by_region if region else self.get_proxies
        proxies = func(region)
        proxy = random.choice(proxies)
        return proxy_str.format(proxy['ip'], proxy['port'])

    def check_proxies(self, proxies=None, insert=False, jail=False, concurrency=20):
        if not proxies:
            proxies = self.proxy_manager.get_proxies()
        chunk_size = int(len(proxies) / concurrency)
        proxy_groups = list(chunks(proxies, chunk_size))
        for group in proxy_groups:
            threading.Thread(target=self._check_proxies, args=(group, insert, jail, )).start()

    def _check_proxies(self, proxies, insert=False, jail=False):
        for proxy in proxies:
            proxy_str = '{}:{}'
            proxy_address = proxy_str.format(proxy['ip'], proxy['port'])
            if proxy_healthy(proxy_address):
                proxy['last_checked'] = datetime.now()
                if insert:
                    self.proxy_manager.insert_proxy(proxy)
                    print('Proxy %s Inserted' % proxy_address)
                elif proxy.get('jailed'):
                    self.proxy_manager.unjail_proxy(proxy)
                    print('Proxy %s Unjailed' % proxy_address)
                else:
                    print('Proxy %s Passed' % proxy_address)
            else:
                if jail:
                    self.proxy_manager.jail_proxy(proxy)
                    print('Proxy %s Jailed' % proxy_address)
                else:
                    print('Proxy %s Failed' % proxy_address)


def proxied_request(url, region=None, **kwargs):
    ps = ProxyService()
    url = url.replace('https:', 'http:')
    proxy = ps.get_random_proxy(region)
    kwargs['proxies'] = {
        'http': proxy,
    }
    return request(kwargs.pop('method'), url, **kwargs), proxy
