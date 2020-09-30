# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from crawler_tools import proxy_ops

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
        spider.logger.warning(f"Request status is {request.status}")
        proxy = proxy_ops.Proxy.get_type_proxy(spider.proxy_conn, 0, 0)
        request.meta['proxy'] = proxy.get_address()
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
        if response.url.find("zen.yandex.ru/id/") != -1:
            global chans_processed
            chans_processed += 1
            spider.logger.warning("Processed %i channel(s) out of 340.000, that's about %F percent done"
                                  % (chans_processed, chans_processed/3400))    # console log
        return response

    def process_exception(self, request, exception, spider):  # TODO FIX нам нужно после каждого ретрая это вызывать)
        # TODO сейчас не так, очевидно. Решается разборками с Retry Middleware, его стоит отключить или перенастроить
        # см https://stackoverflow.com/questions/20805932/scrapy-retry-or-redirect-middleware
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.
        if (request.meta['proxy'].find('https') == -1)and(request.meta['proxy'] != None)and(request.meta['proxy'] != ''):
            request.meta['proxy'].replace('http', 'https')
        else:
            proxy_ops.Proxy.get_from_string(spider.connection, request.meta['proxy']).blacklist(spider.connection)
            # TODO очевидно, строчка выше не работает)) Принты и гугломашина в деле. Кажется, поправили -
            # лохо был запрос к БД написан
            proxy = proxy_ops.Proxy.get_type_proxy(spider.connection, 0, 0)
            request.meta['proxy'] = proxy.get_address()
        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        return request

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
