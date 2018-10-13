from time import sleep
from ProxyService import ProxyService, ProxyManager


def health_service(minutes=15):
        proxy_service = ProxyService()
        proxy_manager = ProxyManager()

        try:
            while(True):
                initial = len(proxy_manager.get_proxies())
                proxy_service.check_proxies()
                final = len(proxy_manager.get_proxies())
                print('Number of healthy proxies changed by %s.' % (final - initial))
                sleep(minutes * 60)
        except:
            print("Shutting down health service.")


def extractor_service(minutes=5):
        proxy_service = ProxyService()
        proxy_manager = ProxyManager()

        try:
            while(True):
                initial = len(proxy_manager.get_proxies())
                proxy_service.update_proxy_list()
                final = len(proxy_manager.get_proxies())
                print('Added %s new proxies.' % (final - initial))
                sleep(minutes * 60)
        except:
            print("Shutting down extractor service.")
