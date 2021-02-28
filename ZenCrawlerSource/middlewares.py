# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy import signals
from scrapy.utils.response import get_meta_refresh
from psycopg2 import InterfaceError
from ZenCrawlerSource.utils.local_resource_manager import DeleGatePortManager, ProxyManager, NoProxiesError, BadProxyException
from twisted.internet.error import ConnectionLost
from twisted.web.http import _DataLoss
from twisted.web._newclient import RequestNotSent, ResponseFailed, ResponseNeverReceived
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from crawler_toolz import proxy_ops, db_ops
import subprocess

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


# class IPTestDownloaderMiddleware(RetryMiddleware): # i mean, we probably don't need retries due to redirect so...
#     @classmethod
#     def from_crawler(cls, crawler):
#         # This method is used by Scrapy to create your spiders.
#         s = cls(crawler.settings)
#         crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
#         return s
#
#     def process_request(self, request, spider):
#         if not request.meta['proxy']:
#             request.meta['proxy'] = 'http://228.228.228.228:1488'
#         return None
#
#     def process_response(self, request, response, spider): # only if we need redirectual retries
#         url = response.url
#         if response.status in [301, 302, 307]:
#             spider.logger.warning(f"We're being redirected with server-side redir {url}")
#             reason = 'redirect %d' % response.status
#             return self._retry(request, reason, spider) or response
#         interval, redirect_url = get_meta_refresh(response)
#         # handle meta redirect
#         if redirect_url:
#             spider.logger.warning(f"We're being redirected with META redir {url}")
#             reason = 'meta'
#             return self._retry(request, reason, spider) or response
#         return response
#
#     def process_exception(self, request, exception, spider): # will always pop if no retry middleware is enabled
#         fine_proxy = 'http://159.203.61.169:8080'
#         request.meta['proxy'] = fine_proxy
#         print(f"Connected to {fine_proxy}")
#         # allows infinite loops. but still, does the thing we desired
#         return request
#
#     def spider_opened(self, spider):
#         spider.logger.info('Spider opened: %s' % spider.name)


class LatestDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def __init__(self):
        self.proxy_manager = ProxyManager()
        self.port_manager = DeleGatePortManager()

    def start_delegated(self, proxy):
        port = self.port_manager.get_free_port()
        self.port_manager.used_ports.append(port)
        if proxy[:6].lower() == "socks4":
            cmd = 'delegated ADMIN=nobody -P:%s SERVER=http TIMEOUT=con:15 SOCKS=%s/-4 -r' % (str(port), proxy)
        else:
            cmd = 'delegated ADMIN=nobody -P:%s SERVER=http TIMEOUT=con:15 SOCKS=%s' % (str(port), proxy)
        subprocess.Popen(cmd, shell=False)
        return str(port)

    def stop_delegated(self, port):
        cmd = 'delegated -P:%s -Fkill' % str(port)
        subprocess.Popen(cmd, shell=False)
        self.port_manager.used_ports.remove(port)

    def proxify(self, request):
        try:
            proxy = self.proxy_manager.get_proxy(proto='http')
        except NoProxiesError:
            try:
                proxy = self.proxy_manager.get_proxy(proto='socks4')
            except NoProxiesError:
                proxy = self.proxy_manager.get_proxy(proto='socks5')
        # say, we managed to get some good proxies (not fallback)
        if proxy.find('socks') == -1:
            request.meta['delegate_port'] = self.start_delegated(request.meta['proxy'])
            # so we change the proxy to our 'middleman' proxy server, it already knows the actual proxy address
            request.meta['proxy'] = 'localhost' + request.meta['delegate_port']
        else:
            # we basically don't need to do a single thing if there's a good-ass http-proxy
            request.meta['proxy'] = proxy

        if proxy.location['country_code'] == 'RU':
            request.headers['Accept-Language'] = 'en-US,en;q=0.9'

    def process_request(self, request, spider):
        if request.headers['Accept-Language'] == 'en-US,en;q=0.9':
            request.headers['Accept-Language'] = 'ru-RU,ru;q=0.9'

        if request.dont_filter:
            request.dont_filter = False

        if 'tries' not in request.meta:
            request.meta['tries'] = 1
        else:
            request.meta['tries'] += 1

        if 'proxy' not in request.meta:
            self.proxify(request)
            return

        # сюда попадаем только когда кончились прокси и мы временно на системном, но засиделись
        if request.meta['tries'] == 6:
            self.proxify(request)

        return

    def process_response(self, request, response, spider):
        if 'delegate_port' in request.meta:
            self.stop_delegated(request.meta['delegate_port'])

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
        spider.logger.warning(exception)

        if 'delegate_port' in request.meta:  # почему бы не словили исключение, если использовали DeleGate - выключаем
            self.stop_delegated(request.meta['delegate_port'])

        if request.meta['tries'] >= 6:  # если мы скрапили на системной проксе, и все равно
            # ловим исключение, даже попытавшись запроксировать еще пару раз, то все хуева :(
            spider.closed("Ran out of proxies, system proxy got blocked, latest proxy used: " + request.meta['proxy'])

        if not request.meta['proxy'] and request.meta['tries'] >= 4:  # если системный прокси прокис, а попыток дохуя
            request.meta['proxy'] = self.proxy_manager.get_fallback_proxy()
            request.dont_filter = True
            return request

        if isinstance(exception, NoProxiesError):
            # когда кончаются прокси, можно скрести на системной проксе, пока не найдутся новые - это +50 минут
            # И угадай что?) Сюда мы попадаем т и тт, когда не можем найти приличных проксей
            request.meta['proxy'] = None  # это чтобы не пытаться проксировать лишнего
            request.dont_filter = True
            return request
        else:
            spider.logger.warning("Request to " + request.url + f" is retrying {request.meta['meta']}th time" )
            if request.meta['proxy']:  # если мы дошли до сюда, то прокся неликвидна (если не на системной)
                request.meta['tries'] -= 1
                self.proxy_manager.blacklist_proxy(request.meta['proxy'])
                del request.meta['proxy']
            request.dont_filter = True
            return request


# class FortyGrandRequestsMiddleware:
#     def __init__(self):  # added connection to db
#         self.db = "proxy_db"
#         self.usr = "postgres"
#         self.pswd = "postgres"
#         self.hst = "127.0.0.1"
#         self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
#         self.current_proxy = self.proxify()
#
#     def ban_proxy(self, request):
#         adr_port = request.meta['proxy'].split("/")[-1]
#         curs = self.conn().cursor
#         req = f"UPDATE proxies SET banned_by_yandex=True WHERE address='{adr_port}';"
#         curs.execute(req)
#         curs.close()
#
#     def proxify(self):
#         req = "SELECT id,address,type FROM proxies WHERE banned_by_yandex = False ORDER BY response_time DESC LIMIT 1;"
#         curs = self.conn.cursor()
#         curs.execute(req)
#         self.conn.commit()
#         p_els = curs.fetchone()
#         curs.close()
#         if p_els:
#             return p_els[3] + "://" + p_els[1]
#         else:
#             return ''
#
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         # This method is used by Scrapy to create your spiders.
#         s = cls()  # also calls __init__
#         crawler.signals.connect(s.open_spider, signal=signals.spider_opened)
#         crawler.signals.connect(s.close_spider, signal=signals.spider_closed)
#         return s
#
#     def open_spider(self, spider):  # isn't being called upon spider's opening))
#         spider.logger.warning(f"self.conn established: {self.conn}")
#
#     def close_spider(self, spider, reason=''):
#         self.conn.close()  # можно сигналом закрывать соединение
#         spider.logger.warning(f"self.conn closed")
#
#     def process_request(self, request, spider):
#         if request.dont_filter:
#             request.dont_filter = False
#         spider.logger.warning("Processing: " +request.url)
#         if 'proxy' not in request.meta:
#             request.meta['proxy'] = self.current_proxy
#             spider.logger.warning(f"Current proxy: {self.current_proxy}")
#         elif request.meta['proxy'] != self.current_proxy:
#             request.meta['proxy'] = self.current_proxy
#             spider.logger.warning(f"Current proxy: {self.current_proxy}")
#         elif request.meta['proxy'] == '':
#             self.current_proxy = self.proxify()
#             request.meta = self.current_proxy
#         return None
#
#     def process_response(self, request, response, spider):
#         spider.logger.warning(f"Response status is {response.status}")
#         if response.url.find("zen.yandex.ru/") != -1 and response.url.find("zen.yandex.ru/media") == -1:
#             global chans_processed
#             chans_processed += 1
#             spider.logger.warning("Processed %i channel(s) out of 400.000, that's about %F percent done"
#                                   % (chans_processed, chans_processed / 4000))  # console log
#         if response.status == 200:
#             return response
#
#         elif response.status not in [404] + [i for i in range(300, 310, 1)]:
#             if 'proxy' in request.meta:
#                 if request.meta['proxy'] != '':
#                     self.ban_proxy(request)
#                 self.current_proxy = self.proxify()
#                 request.meta['proxy'] = self.current_proxy
#                 request.dont_filter = True
#                 return request
#             else:
#                 self.current_proxy = self.proxify()
#                 request.meta['proxy'] = self.current_proxy
#                 request.dont_filter = True
#                 return request
#
#         # elif response.status in [i for i in range(300, 310, 1)]:
#         #     if response.url !=
#
#         elif response.status == 404: # вообще-то, подозрительно) TODO если их будет оч много, научимся обходить)
#             return None  # or shall we return response...
#
#         else: # на всякий случай))))))
#             raise Exception
#
#     # TODO доделать тут
#     def process_exception(self, request, exception, spider):
#         try:
#             spider.logger.warning(exception + " happened, so we re-checking proxies, crawling halted")
#             if request.meta['proxy'] != '' or exception in [RequestNotSent, ResponseFailed, ResponseNeverReceived]:
#                 self.ban_proxy(request)
#                 check_in_db(self.conn)
#                 self.current_proxy = self.proxify()
#                 request.meta['proxy'] = self.current_proxy
#                 spider.logger.warning("Successfully found new proxy, keep on with " + self.current_proxy)
#                 if self.current_proxy == '':
#                     spider.logger.warning("Ran out of proxies, can't continue")
#                     self.close_spider(spider, "No proxies to crawl with")
#             elif not self.conn:
#                 raise AttributeError
#
#         except InterfaceError:
#             spider.logger.warning("Could not connect to db, conn closed, re-establishing")
#             self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
#             if request.meta['proxy'] != '':
#                 self.ban_proxy(request)
#                 check_in_db(self.conn)
#                 self.current_proxy = self.proxify()
#                 request.meta['proxy'] = self.current_proxy
#
#         except AttributeError:   # could lead to more complicated bugs, but it'll do just fine if works
#             spider.logger.warning("Lost connection to proxy_db, re-establishing")
#             self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
#             if request.meta['proxy'] != '':
#                 self.ban_proxy(request)
#                 check_in_db(self.conn)
#                 self.current_proxy = self.proxify()
#                 request.meta['proxy'] = self.current_proxy
#
#         request.dont_filter = True
#         return request
###############################################################################


# class IPNoRetryDownloaderMiddleware: - deprecated
class ZencrawlersourceDownloaderMiddleware:  # i mean, we don't really need retries due to redirect so...
    def __init__(self):  # added connection to db
        self.db = "proxies"
        self.usr = "postgres"
        self.pswd = "postgres"
        self.hst = "127.0.0.1"
        self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)

    def ban_proxy(self, request):
        adr_port = request.meta['proxy'].split("/")[-1]
        addr, port = adr_port.split(":")[0], adr_port.split(":")[1]
        curs = self.conn().cursor
        req = f"UPDATE proxies SET banned_by_yandex=True WHERE address='{addr}' AND port={int(port)};"
        curs.execute(req)
        curs.close()

    def proxify(self):
        req = "SELECT id,address,port FROM proxies WHERE banned_by_yandex = False AND (SELECT bad_count FROM details WHERE proxy_id = id) = 0 ORDER BY id DESCLIMIT 1;"
        curs = self.conn.cursor()
        curs.execute(req)
        self.conn.commit()
        return curs.fetchone()[3] + "://" + curs.fetchone()[1] + ":" + str(curs.fetchone()[2])
        curs.close()


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
        if request.dont_filter: # fixes infinite retrying TODO check if that's correct
            request.dont_filter = False

        spider.logger.warning("Processing request (see url below)")
        spider.logger.warning(request.url)
        if 'proxy' not in request.meta or request.meta['proxy'] == '':
            # picking a proxy TODO vary parameters of picking
            proxy_string = self.proxify()
            request.meta['proxy'] = proxy_string
            spider.logger.warning(f"Proxy is set to {proxy_string}")
        return None

    def process_response(self, request, response, spider):
        spider.logger.warning(f"Response status is {response.status}")
        if response.url.find("zen.yandex.ru/") != -1 and response.url.find("zen.yandex.ru/media") == -1: # test w/ TOR TODO
            global chans_processed
            chans_processed += 1
            spider.logger.warning("Processed %i channel(s) out of 340.000, that's about %F percent done"
                                  % (chans_processed, chans_processed / 3400))  # console log
        # 4xx errors handler
        if response.status == 200:
            return response

        elif response.status in [407, 409, 500, 501, 502, 503, 508, 301, 302, 307, 303, 304]:
        # could cause endless loop and eventual loss of information..... TODO 3xx support
            if 'proxy' in request.meta:  # checks that key exists
                if request.meta['proxy'] != '':
                    self.ban_proxy(request)
                    # proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                # request.meta['proxy'] = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                request.meta['proxy'] = self.proxify()
                request.dont_filter = True # TODO endless looping? SoBayed
                return request
            else: # don't actually need that
                # request.meta['proxy'] = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                request.meta['proxy'] = self.proxify()
                request.dont_filter = True
                return request

        elif response.status == 404:
            return None # or shall we return response...

        else:  # то есть нужно по-хорошему тестить уже на дзенчике, вдруг умники с яндекса
            # отдадут вечный 3хх или 404) ну посмотрим, посмотрим
            # или бляццкую пустую страницу
            raise Exception

    def process_exception(self, request, exception, spider):
        try:
            if request.meta['proxy'] != '':  # if there's a proxy, it's a bad one
                # blacklisting a proxy
                self.ban_proxy(request)
                # proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = ''  # proxy.get_address()
                # spider.logger.warning(f"{self.conn.closed}") # returns 0 or 1
            # proxy = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
            # TODO возожно, здесь стоит брать новую рандомную проксю, так мб будет быстрее
            elif not self.conn:
                raise AttributeError

        except KeyError:  # always getting triggered. TODO rework rotation logic around this
            # traceback.print_exc()
            request.meta['proxy'] = ''
            spider.logger.warning("Forced proxy exception")

        except InterfaceError:
            spider.logger.warning("Could not connect to db, conn closed, re-establishing")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            if request.meta['proxy'] != '':
                self.ban_proxy(request)
                # proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = ''

        except AttributeError: # could lead to more complicated bugs, but it'll do just fine if works
            spider.logger.warning("finally, AttributeError")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)

        request.dont_filter = True # makes process_request work on handled request
        return request

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
        elif response.status in [407, 409, 500, 501, 502, 503, 508, 301, 302, 307, 303, 304]:

            if 'proxy' in request.meta: # checks that key exists
                if request.meta['proxy'] != '':
                    proxy_ops.Proxy.get_from_string(self.conn, request.meta['proxy']).blacklist(self.conn)
                request.meta['proxy'] = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                return request
            else:
                request.meta['proxy'] = proxy_ops.Proxy.get_type_proxy(self.conn, 0, 0)
                return request

        elif response.status == 404:
            return response

        else: # то есть нужно по-хорошему тестить уже на дзенчике, вдруг умники с яндекса
            # отдадут вечный 3хх или 404) ну посмотрим, посмотрим
            # или бляццкую пустую страницу
            raise Exception

    def process_exception(self, request, exception, spider):
        try:
            if 'proxy' not in request.meta:
                request.meta['proxy'] = ''
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