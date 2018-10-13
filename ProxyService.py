import threading
import random
from time import sleep
from requests import request, get
from datetime import datetime
from pymongo import MongoClient
from ManyRequests.WebRunner import WebRunner
from extraction import extract_freeproxy_page
from multiprocessing import Process


def health_service(minutes=15):
    proxy_service = ProxyService()
    proxy_manager = ProxyManager()

    try:
        while (True):
            initial = len(proxy_manager.get_proxies())
            proxy_service.check_proxies()
            final = len(proxy_manager.get_proxies())
            print('Number of healthy proxies changed by %s.' % (
                        final - initial))
            sleep(minutes * 60)
    except:
        print("Shutting down health service.")


def extractor_service(minutes=5):
    proxy_service = ProxyService()
    proxy_manager = ProxyManager()

    try:
        while (True):
            initial = len(proxy_manager.get_proxies())
            proxy_service.update_proxy_list()
            final = len(proxy_manager.get_proxies())
            print('Added %s new proxies.' % (final - initial))
            sleep(minutes * 60)
    except:
        print("Shutting down extractor service.")


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
        self.check_proxies()
        self.update_proxy_list()

    def update_proxy_list(self):
        _proxies = extract_freeproxy_page()
        if len(_proxies) == 0:
            print('Failed to download proxies.')
            return
        self.health_check(_proxies)

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

    def check_proxies(self):
        self.health_check(self.proxy_manager.get_proxies())


    def health_check(self, proxy_list):
        proxy_urls = ['http://%s:%s' % (proxy_dict['ip'], proxy_dict['port']) for
                      proxy_dict in proxy_list]
        proxies = [{'http': p, 'https': p} for p in proxy_urls]
        ping_urls = ['http://www.google.com'] * len(proxies)

        runner = WebRunner()
        responses = runner.run(ping_urls, concurrency=100, proxies=proxies,
                               timeout=10)
        for proxy, response in zip(proxy_list, responses):
            if response is None or response.status_code != 200:
                self.jail(proxy)
            else:
                self.increase_health(proxy)


    def jail(self, proxy):
        self.proxy_manager.jail_proxy(proxy)


    def increase_health(self, proxy):
        if not self.exists(proxy):
            self.proxy_manager.insert_proxy(proxy)
        self.proxy_manager.unjail_proxy(proxy)


    def exists(self, proxy):
        return self.proxy_manager.get_proxy(proxy) is not None


def proxied_request(url, region=None, **kwargs):
    ps = ProxyService()
    url = url.replace('https:', 'http:')
    proxy = ps.get_random_proxy(region)
    kwargs['proxies'] = {
        'http': proxy,
    }
    return request(kwargs.pop('method'), url, **kwargs), proxy


def start_service():
    hs = Process(target=health_service)
    es = Process(target=extractor_service)
    hs.start()
    es.start()


if __name__ == '__main__':
    start_service()