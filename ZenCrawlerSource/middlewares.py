# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy import signals
from scrapy.utils.response import get_meta_refresh
from psycopg2 import InterfaceError
import traceback

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from crawler_toolz import proxy_ops, db_ops

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


class ZencrawlersourceDownloaderMiddlewareArchive:
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
            proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
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
                proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
            # proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
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


# class IPNoRetryDownloaderMiddleware: - deprecated
class ZencrawlersourceDownloaderMiddleware:  # i mean, we don't really need retries due to redirect so...
    def __init__(self):  # added connection to db
        self.db = "proxy_db"
        self.usr = "postgres"
        self.pswd = "postgres"
        self.hst = "127.0.0.1"
        self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()   # also calls __init__
        crawler.signals.connect(s.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(s.close_spider, signal=signals.spider_closed)
        return s

    def open_spider(self, spider): # isn't being called upon spider's opening))
        spider.logger.warning(f"self.conn established: {self.conn}") # TODO that's a new ONE! check it

    def close_spider(self, spider):
        self.conn.close()  # можно сигналом закрывать соединение
        spider.logger.warning(f"self.conn closed")

    def process_request(self, request, spider):
        if request.dont_filter: # fixes infinite retrying
            request.dont_filter = False

        spider.logger.warning("Processing request (see url below)")
        spider.logger.warning(request.url)
        if request.meta['proxy'] == '' or not request.meta['proxy']:
            proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
            proxy_string = proxy.get_address()
            request.meta['proxy'] = proxy_string
            spider.logger.warning(f"Proxy is set to {proxy_string}")
        return None

    def process_response(self, request, response, spider):
        spider.logger.warning(f"Response status is {response.status}")
        if response.url.find("zen.yandex.ru/id/") != -1:
            global chans_processed
            chans_processed += 1
            spider.logger.warning("Processed %i channel(s) out of 340.000, that's about %F percent done"
                                  % (chans_processed, chans_processed / 3400))  # console log
        # 4xx errors handler
        if response.status == 200:
            return response
        elif response.status in [407, 409, 500, 501, 502, 503, 508]:
            proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
            request.meta['proxy'] = ''
            return request
        else: # то есть нужно по-хорошему тестить уже на дзенчике, вдруг умники с яндекса
            # отдадут вечный 3хх или 404) ну посмотрим, посмотрим
            return response

    def process_exception(self, request, exception, spider):
        try:
            if request.meta['proxy'] != '':  # if there's a proxy, it's a bad one
                proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = ''  # proxy.get_address()
                # spider.logger.warning(f"{self.conn.closed}") # returns 0 or 1
            # proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
            # TODO возожно, здесь стоит брать новую рандомную проксю, так мб будет быстрее
            elif not self.conn:
                raise AttributeError

        except KeyError:  # always getting triggered. TODO rework rotation logic around this
            # traceback.print_exc()
            request.meta['proxy'] = ''
            spider.logger.warning(f"WOW! Look at that {exception} happened, but we're here due to KeyError")

        except InterfaceError:
            spider.logger.warning("Could not connect to db, conn closed, re-establishing")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            if request.meta['proxy'] != '':
                proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = ''

        except AttributeError: # could lead to more complicated bugs, but it'll do just fine if works
            spider.logger.warning("finally, AttributeError")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
        request.dont_filter = True # makes process_request work on handled request
        return request # такое чувство, что вот здесь хуйня фильтрует лишнего. типа когда снимаешь фильтры,
        # запросы норм идут

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class OnlyExceptionsProxified:
    def __init__(self):  # added connection to db
        self.db = "proxy_db"
        self.usr = "postgres"
        self.pswd = "postgres"
        self.hst = "127.0.0.1"
        self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()   # also calls __init__
        crawler.signals.connect(s.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(s.close_spider, signal=signals.spider_closed)
        return s

    def open_spider(self, spider): # isn't being called upon spider's opening))
        spider.logger.warning(f"self.conn established: {self.conn}")

    def close_spider(self, spider):
        self.conn.close()  # можно сигналом закрывать соединение
        spider.logger.warning(f"self.conn closed")

    def process_request(self, request, spider):
        if request.dont_filter: # fixes infinite retrying
            request.dont_filter = False
        return None

    def process_response(self, request, response, spider):
        spider.logger.warning(f"Response status is {response.status}")
        if response.url.find("zen.yandex.ru/id/") != -1:
            global chans_processed
            chans_processed += 1
            spider.logger.warning("Processed %i channel(s) out of 340.000, that's about %F percent done"
                                  % (chans_processed, chans_processed / 3400))  # console log
        # 4xx errors handler
        if response.status == 200:
            return response
        elif response.status in [407, 409, 500, 501, 502, 503, 508, 301, 302, 307]:
            if 'proxy' in request.meta: # checks that key exists
                if request.meta['proxy'] != '':
                    proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                return request
            else:
                raise Exception
        else: # то есть нужно по-хорошему тестить уже на дзенчике, вдруг умники с яндекса
            # отдадут вечный 3хх или 404) ну посмотрим, посмотрим
            # или бляццкую пустую страницу
            return response

    def process_exception(self, request, exception, spider):
        try:
            if 'proxy' not in request.meta:
                request.meta['proxy'] == ''
            if request.meta['proxy'] != '':  # if there's a proxy, it's a bad one
                proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                request.meta['proxy'] = proxy.get_address()
            # TODO возожно, здесь стоит брать новую рандомную проксю, так мб будет быстрее
            elif not self.conn:
                raise AttributeError
            else:
                proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                request.meta['proxy'] = proxy.get_address()

        except KeyError:  # always getting triggered. TODO rework rotation logic around this
            # traceback.print_exc()
            request.meta['proxy'] = ''
            spider.logger.warning(f"WOW! Look at that {exception} happened, but we're here due to KeyError")

        except InterfaceError:
            spider.logger.warning("Could not connect to db, conn closed, re-establishing")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            if request.meta['proxy'] != '':
                proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = ''

        except AttributeError: # could lead to more complicated bugs, but it'll do just fine if works
            spider.logger.warning("finally, AttributeError")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
        request.dont_filter = True # makes process_request work on handled request
        return request # такое чувство, что вот здесь хуйня фильтрует лишнего. типа когда снимаешь фильтры,
        # запросы норм идут