# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy import signals
from scrapy.utils.response import get_meta_refresh

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from crawler_toolz import proxy_ops

chans_processed = 0


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


class ZencrawlersourceDownloaderMiddleware:
    # def __init__(self):
    #     self.zen_conn = db_ops.connect_to_db("proxy_db", "postgres", "postgres", "127.0.0.1")
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.
        # Для теста можно bad_checks = 2)) Тогда точно ошибка вылезет, заблэклистим и сменим
        spider.logger.warning("Processing request...")
        if request.meta['proxy'] == '':
            proxy = proxy_ops.Proxy.get_type_proxy(spider.proxy_conn, 0, 0)
            proxy_string = proxy.get_address()
            request.meta['proxy'] = proxy_string
            spider.logger.warning(f"Proxy is set to {proxy_string}")
        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.
        # request.meta['proxy'] = 'http://203.202.245.58:80'
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        spider.logger.warning(f"Request status is {response.status}")
        if response.url.find("zen.yandex.ru/id/") != -1:
            global chans_processed
            chans_processed += 1
            spider.logger.warning("Processed %i channel(s) out of 340.000, that's about %F percent done"
                                  % (chans_processed, chans_processed / 3400))  # console log
        return response

    def process_exception(self, request, exception, spider):
        # см https://stackoverflow.com/questions/20805932/scrapy-retry-or-redirect-middleware
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.
        try:
            if request.meta['proxy'] != '':
                proxy_ops.Proxy.get_from_string(spider.proxy_conn, request.meta['proxy']).blacklist(spider.proxy_conn)
            # proxy = proxy_ops.Proxy.get_type_proxy(spider.proxy_conn, 0, 0)
            # TODO возожно, здесь стоит брать новую рандомную проксю, так мб будет быстрее
            request.meta['proxy'] = ''  # proxy.get_address()
        except KeyError:
            pass
        #   self.process_request(request, spider)
        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        return request

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class IPTestDownloaderMiddleware(RetryMiddleware): # i mean, we probably don't need retries due to redirect so...
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler.settings)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        if not request.meta['proxy']:
            request.meta['proxy'] = 'http://228.228.228.228:1488'
        return None

    def process_response(self, request, response, spider): # only if we need redirectual retries
        url = response.url
        if response.status in [301, 302, 307]:
            spider.logger.warning(f"We're being redirected with server-side redir {url}")
            reason = 'redirect %d' % response.status
            return self._retry(request, reason, spider) or response
        interval, redirect_url = get_meta_refresh(response)
        # handle meta redirect
        if redirect_url:
            spider.logger.warning(f"We're being redirected with META redir {url}")
            reason = 'meta'
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider): # will always pop if no retry middleware is enabled
        fine_proxy = 'http://159.203.61.169:8080'
        request.meta['proxy'] = fine_proxy
        print(f"Connected to {fine_proxy}")
        # allows infinite loops. but still, does the thing we desired
        return request

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class IPNoRetryDownloaderMiddleware:  # i mean, we probably don't need retries due to redirect so...
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        spider.logger.warning("Processing request...")
        if request.meta['proxy'] == '' or not request.meta['proxy']:
            proxy = proxy_ops.Proxy.get_type_proxy(spider.proxy_conn, 0, 0)
            proxy_string = proxy.get_address()
            request.meta['proxy'] = proxy_string
            spider.logger.warning(f"Proxy is set to {proxy_string}")
        return None

    def process_response(self, request, response, spider):
        spider.logger.warning(f"Request status is {response.status}")
        if response.url.find("zen.yandex.ru/id/") != -1:
            global chans_processed
            chans_processed += 1
            spider.logger.warning("Processed %i channel(s) out of 340.000, that's about %F percent done"
                                  % (chans_processed, chans_processed / 3400))  # console log
        return response

    def process_exception(self, request, exception, spider):
        try:
            if request.meta['proxy'] != '' or request.meta['proxy']:
                proxy_ops.Proxy.get_from_string(spider.proxy_conn, request.meta['proxy']).blacklist(spider.proxy_conn)
            # proxy = proxy_ops.Proxy.get_type_proxy(spider.proxy_conn, 0, 0)
            # TODO возожно, здесь стоит брать новую рандомную проксю, так мб будет быстрее
            request.meta['proxy'] = ''  # proxy.get_address()
        except KeyError:  # always getting triggered. TODO rework rotation logic around this
            request.meta['proxy'] = ''
            spider.logger.warning(f"WOW! Look at that {exception} happened, but we're here due to KeyError")
        return request

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
