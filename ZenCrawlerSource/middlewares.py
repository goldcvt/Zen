# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy import signals
from scrapy.utils.response import get_meta_refresh
from ZenCrawlerSource.utils.local_resource_manager import DeleGatePortManager, ProxyManager, NoProxiesError, BadProxyException
from twisted.internet.error import ConnectionLost
from twisted.web.http import _DataLoss
from twisted.web._newclient import RequestNotSent, ResponseFailed, ResponseNeverReceived
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
# from crawler_toolz import proxy_ops, db_ops
import subprocess


class ZencrawlersourceSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class LatestDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.open_spider, signal=signals.spider_opened)
        return s

    def open_spider(self, spider): # isn't being called upon spider's opening))
        spider.logger.warning(f"Starting...")

    def __init__(self):
        self.proxy_manager = ProxyManager()
        self.port_manager = DeleGatePortManager()

    def start_delegated(self, proxy):
        port = self.port_manager.get_free_port()
        self.port_manager.reserve_port(port)
        if proxy[:6].lower() == "socks4":
            cmd = ['/usr/bin/delegated', 'ADMIN=nobdoy', f'-P:{str(port)}', 'SERVER=http', 'TIMEOUT=con:15',
                   f'SOCKS={proxy}/-4',  '-r']
        else:
            cmd = ['/usr/bin/delegated', 'ADMIN=nobdoy', f'-P:{str(port)}', 'SERVER=http', 'TIMEOUT=con:15',
                   f'SOCKS={proxy}']
        subprocess.Popen(cmd, shell=False)
        return str(port), proxy

    def stop_delegated(self, port):
        cmd = ['/usr/bin/delegated', f'-P:{str(port)}', '-Fkill']
        subprocess.Popen(cmd, shell=False)
        print(F"KILLED DELEGATE AT {str(port)}")
        self.port_manager.release_port(port)

    def proxify(self, request):
        try:
            proxy, loc = self.proxy_manager.get_proxy(proto='http', bad_checks=0)
        except NoProxiesError:
            try:
                proxy, loc = self.proxy_manager.get_proxy(proto='socks4', bad_checks=0)
            except NoProxiesError:
                proxy, loc = self.proxy_manager.get_proxy(proto='socks5', bad_checks=0)
        # say, we managed to get some good proxies (not fallback)
        if proxy.find('socks') != -1:
            request.meta['delegate_port'], request.meta['proxy_origin'] = self.start_delegated(proxy)
            # so we change the proxy to our 'middleman' proxy server, it already knows the actual proxy address
            request.meta['proxy'] = 'http://0.0.0.0:' + request.meta['delegate_port']
        else:
            # we basically don't need to do a single thing if there's a good-ass http-proxy
            request.meta['proxy'] = proxy
        print("got proxies")
        print(request.meta['proxy'])
        if loc == 'RU':
            request.headers['Accept-Language'] = 'en-US,en;q=0.9'

    def process_request(self, request, spider):
        if request.headers.get('Accept-Language') == 'en-US,en;q=0.9':
            request.headers['Accept-Language'] = 'ru-RU,ru;q=0.9'

        if request.dont_filter:
            request.dont_filter = False

        if 'tries' not in request.meta:
            request.meta['tries'] = 1
        else:
            request.meta['tries'] += 1

        if 'proxy' not in request.meta:
            spider.logger.warn("Trying to proxify")
            self.proxify(request)
            return

        # сюда попадаем только когда кончились прокси и мы временно на системном, но засиделись
        if request.meta['tries'] == 6:
            self.proxify(request)

        return

    def process_response(self, request, response, spider):
        if 'delegate_port' in request.meta:
            self.stop_delegated(request.meta['delegate_port'])
            del request.meta['delegate_port']

        if response.status == 200:
            return response
        elif response.status in [301, 302, 307, 303, 304, 309]:
            spider.logger.warning(f"Got REDIRECT {response.status}, giving up")
        elif response.status == 404:
            spider.logger.warning("Got NOT FOUND 404")
        # elif response.status in [407, 409, 500, 501, 502, 503, 508]:
        else:
            spider.logger.warning(f"Got a code {response.status} response, declaring proxy dead")
            raise BadProxyException

    def process_exception(self, request, exception, spider):  # TODO test
        spider.logger.warning("PROCESSING EXCEPTION")
        spider.logger.warning(exception)

        if 'delegate_port' in request.meta:  # почему бы не словили исключение, если использовали DeleGate - выключаем
            self.stop_delegated(request.meta['delegate_port'])
            del request.meta['delegate_port']

        if request.meta['tries'] >= 6:  # если мы скрапили на системной проксе, и все равно
            # ловим исключение, даже попытавшись запроксировать еще пару раз, то все хуева :(
            spider.closed("Ran out of proxies, system proxy got blocked, latest proxy used: " + request.meta['proxy'])

        if not request.meta['proxy'] and request.meta['tries'] >= 4:  # если системный прокси прокис, а попыток дохуя
            proxy = self.proxy_manager.get_fallback_proxy()

            if proxy.find('socks') != -1:
                request.meta['delegate_port'], request.meta['proxy_origin'] = self.start_delegated(
                    request.meta['proxy']
                )
                # so we change the proxy to our 'middleman' proxy server, it already knows the actual proxy address
                request.meta['proxy'] = 'http://0.0.0.0:' + request.meta['delegate_port']
            else:
                # we basically don't need to do a single thing if there's a good-ass http-proxy
                request.meta['proxy'] = proxy

            request.dont_filter = True
            return request

        if isinstance(exception, NoProxiesError):
            # когда кончаются прокси, можно скрести на системной проксе, пока не найдутся новые - это +50 минут
            # И угадай что?) Сюда мы попадаем т и тт, когда не можем найти приличных проксей
            request.meta['proxy'] = None  # это чтобы не пытаться проксировать лишнего
            request.dont_filter = True
            return request
        else:
            spider.logger.warning("Request to " + request.url + f" is retrying {request.meta['tries']}th time" )
            if request.meta['proxy']:  # если мы дошли до сюда, то прокся неликвидна (если не на системной)
                request.meta['tries'] -= 1
                if 'proxy_origin' in request.meta:
                    self.proxy_manager.blacklist_proxy(request.meta['proxy_origin'])
                    del request.meta['proxy_origin']
                else:
                    self.proxy_manager.blacklist_proxy(request.meta['proxy'])
                del request.meta['proxy']
            request.dont_filter = True
            return request
