from time import sleep
from .ProxyService import ProxyService, ProxyManager


class ExtractorService:
    def __init__(self):
        self.proxy_service = ProxyService()
        self.proxy_manager = ProxyManager()

    def start(self):
        try:
            while(True):
                initial = len(self.proxy_manager.get_proxies())
                self.proxy_service.refresh_proxies()
                final = len(self.proxy_manager.get_proxies())
                print('Added %s new proxies.' % (final - initial))
                sleep(15 * 60)
        except:
            print("Shutting down extractor service.")


if __name__ == '__main__':
    es = ExtractorService()
    es.start()
