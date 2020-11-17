import scrapy
from crawler_toolz import db_ops
import datetime
import re
from ZenCrawlerSource.items import ChannelItem, ArticleItem, ZencrawlersourceItem
import json
from tqdm import tqdm
from psycopg2 import InterfaceError

non_arbitrage = ['instagram.com', 'twitter.com']


class Articles():

    def __init__(self, date, header, url, arb_link='', arbitrage=False, streaming=False, form=False):
        self.publication_date = date
        self.header = header
        self.url = url
        self.arb_link = arb_link
        self.arbitrage = arbitrage
        self.form = form
        self.streaming = streaming

    def __str__(self):
        return f'{str(vars(self))}'

    def __repr__(self):
        return f'{str(vars(self))}'

    def using_form(self, response):
        forms = response.css("div.yandex-forms-embed").get()
        streaming = response.css("div.yandex-efir-embed").get()
        other_embeds = response.css("div.article-render__block_embed").get() # bug fixed
        if streaming:
            self.streaming = True
        if forms or other_embeds:
            self.form = True
            self.arbitrage = True

    def is_arbitrage(self, response): # checks straight-up links
        if_p = response.css("p.article-render__block a.article-link::attr(href)").get()
        if_blockquote = response.css("blockquote.article-render__block a.article-link::attr(href)").get()
        tmp = False

        # for i in non_arbitrage: # TODO откомментить, если много мусора ссылочного
        #     if if_p.find(i) != -1 or if_blockquote.find(i) != -1:
        #         tmp = True
        #         break

        if if_p or if_blockquote and (not tmp):
            self.arbitrage = True
            self.arb_link = if_p or if_blockquote # питонно пиздец)
        self.using_form(response)


class Channels():
    # arbitrage: bool

    def __init__(self, subs, audience, url, links=[], articles=[], arbitrage=False, form=False, is_crawled=False, streaming=False):
        self.subs = int(subs)
        self.audience = int(audience)
        self.url = url
        self.links = links
        self.articles = articles
        self.arbitrage = arbitrage
        self.form = form
        self.is_crawled = is_crawled
        self.is_streaming = streaming

    def __str__(self):
        my_dict = vars(self)
        return f'{str(my_dict)}'

    @staticmethod
    def parse_description(response):
        desc_links = response.css("div.zen-app div.channel-header-view-desktop__description-block a::attr(href)").getall()
        desc_text = response.css("div.zen-app div.channel-header-view-desktop__description-block p::text").get()
        emails = None
        if desc_text:
            emails = re.findall("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", desc_text)

        if emails and desc_links:
            return desc_links + emails
        elif emails:
            return emails
        elif desc_links:
            return desc_links
        else:
            return []

    def get_contacts(self, response):
        contacts = response.css("div.zen-app div.social-links-view__wrapper li a::attr(href)").getall()
        contacts += Channels.parse_description(response)
        if contacts:
            self.links = contacts

    def is_arbitrage(self, number_of_articles):
        i = 0
        for article in self.articles:
            if article.arbitrage:
                i += 1
            if article.form and not self.form:
                self.form = True
            if article.streaming:
                self.is_streaming = True

        if i/number_of_articles >= 0.5:
            self.arbitrage = True
        else:
            self.arbitrage = False

    def if_crawled(self, conn): # чекаем, что уже есть в нашей дб) тогда тащем-та столбец my не имеет смысла
        found = db_ops.read_from_db(conn, "channels", "channel_id", where="url='{}'".format(self.url))
        # DEBUG а мы что возвращаем?)
        if found:
            self.is_crawled = True
            return True
        else:
            return False


class ExampleSpider(scrapy.Spider):
    name = "nightcrawler"

    allowed_domains = ["zen.yandex.ru", "zen.yandex.com"]
    start_urls = ["https://zen.yandex.ru/media/zen/channels"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.zen_conn = db_ops.connect_to_db("zen_copy", "postgres", "postgres", "127.0.0.1")
        self.logger.warning("Established spider-based connection to zen_copy")

    def parse(self, response):

        for a in tqdm(response.css("div.alphabet__list a.alphabet__item::attr(href)").getall()):
            if a != "media/zen/channels": # DONE теперь итерация правильная - TODO
                yield response.follow(a, callback=self.parse_by_letter, dont_filter=True)

    def parse_by_letter(self, response):
        channel_top = response.css("a.channel-item__link").get()
        if channel_top: # DONE чекни, мб мы проебываем 1 страницу выдачи в каждой - TODO
            next_page = response.css(
                "div.pagination-prev-next__button a.pagination-prev-next__link::attr(href)").getall()
            if len(next_page) > 1:

                yield response.follow(next_page[-1], callback=self.parse_by_letter)

                chans = response.css("a.channel-item__link::attr(href)").getall()
                for chan in chans:
                    yield response.follow(chan, callback=self.parse_channel)

            elif len(next_page) == 1:

                yield response.follow(next_page[0], callback=self.parse_by_letter)

                chans = response.css("a.channel-item__link::attr(href)").getall()
                for chan in chans:
                    yield response.follow(chan, callback=self.parse_channel)

    def parse_channel(self, response): # DONE перевели на классы - TODO
        self.logger.warning("Channel name: " + response.css("div.zen-app div.channel-header-view-desktop__info-block h1 span::text").get())
        default_stats = response.css("div.zen-app div.channel-info-view__block div.channel-info-view__value::text").getall()
        stat_kword = response.css("div.zen-app div.channel-info-view__block div.channel-info-view__name::text").get()
        # DONE implemented PC UA TODO
        if len(default_stats) == 2:
            subs = int("".join(("".join(default_stats[0].split("<"))).split(" ")))
            audience = int("".join(("".join(default_stats[1].split("<"))).split(" ")))
        else:
            if stat_kword.find("одписч") != -1:
                subs = int("".join(("".join(default_stats[0].split("<"))).split(" ")))
                audience = 0
            elif stat_kword.find("удитори") != -1:
                audience = int("".join(("".join(default_stats[0].split("<"))).split(" ")))
                subs = 0
            else:
                audience = 0
                subs = 0

        chan = Channels(subs, audience, response.url)
        chan.get_contacts(response)

        try:
            chan.if_crawled(self.zen_conn)
            self.logger.warning(f"Checking whether {chan.url} was parsed")
            if chan.is_crawled:
                self.logger.warning(f"{chan.url} have been parsed before")
            else:
                self.logger.warning(f"{chan.url} haven't been parsed before")
        except InterfaceError:
            self.zen_conn = db_ops.connect_to_db("zen_copy", "obama", "obama", "127.0.0.1")
            chan.if_crawled(self.zen_conn)
            if chan.is_crawled:
                self.logger.warning(f"{chan.url} have been parsed before")
            else:
                self.logger.warning(f"{chan.url} haven't been parsed before")

        # can move that line to top and make if statement, so we only get channels w/ articles to bd
        urls = response.css("div.card-wrapper__inner a.card-image-view__clickable::attr(href)").getall()[:5]
        # CHANGE x in [:x] for different MAX amount of articles to be fetched

        self.logger.warning(f"Itemizing {chan}")
        my_item = self.itemize_channel(chan)
        yield my_item

        for url in urls:
            if url.find("zen.yandex.ru"):   # мало ли, вдруг мы зашли на сайтовый канал
                yield response.follow(url,
                                      callback=self.fetch_article
                                      )
        # del chan

        # yield from response.follow_all(urls,
        #                                callback=self.fetch_article,
        #                                cb_kwargs=dict(channel=chan, total_articles=len(urls))
        #                                )
        # article_urls = response.css("div.card-wrapper__inner a::attr(href)").getall()
        # articles = []
        #
        # for i in range(0, 4, 1):
        #     articles.append(self.fetch_article(response, article_urls[i])) # TODO протестить DONE
        #
        # chan.articles = articles
        # chan.is_arbitrage(articles)
        #
        # yield ExampleSpider.pack_to_items(chan)
        # del chan
        # yield from response.follow_all(articles, callback=self.fetch_article)

    # TODO статистика подгружается джаваскриптом... В отличии от канальной. В первой версии она не критична

    def fetch_article(self, response):
        title = response.css("div#article__page-root h1.article__title::text").get()
        d_str = response.css("footer.article__statistics span.article-stat__date::text").get()
        date = datetime.datetime(1900, 12, 12, 12, 12, 12, 0)
        if d_str:
            date = ExampleSpider.get_date(d_str)
        # url = response.url
        # if url.find("/id/") != -1:  # TODO change items accordingly. Move everything about article to
        #  get_reads or
        #     # TODO find a way to get actual reads and views
        #     art_id = "".join(url.split("-")[-1])
        #     author_id = "".join(url.split("/")[-2])
        #     reads, views = response.follow(f"https://zen.yandex.ru/media-api/
        #     publication-view-stat?publicationId={art_id}" +
        #                                    f"&publisherId={author_id}", callback=self.get_reads)
        # else:
        #
        #     pass # have to use JS :(

        article = Articles(date, title, response.url)
        article.is_arbitrage(response)
        art_item = self.itemize_article(article)
        yield art_item


    def get_reads(self, response):
        resp_string = "{}".format(response.css("body p").get())
        my_dict = json.loads(resp_string)
        reads = my_dict["viewsTillEnd"]
        views = my_dict["views"]
        return reads, views


    @staticmethod
    def itemize_channel(channel):
        # articles = [ExampleSpider.article_to_item(article) for article in channel.articles]
        item = ChannelItem(
                                subs=channel.subs,
                                audience=channel.audience,
                                url=channel.url,
                                contacts=channel.links, # может вызывать проблемы
                                # is_arbitrage=channel.arbitrage, # в целом не нужно, тут же всегда будет False
                                # form=channel.form, # и тут
                                # whether_crawled=channel.if_crawled,
                                last_checked=datetime.datetime.now(),
                                # is_streaming=channel.is_streaming # и тут
        )
        return item

    @staticmethod
    def itemize_article(article):
        item = ArticleItem(
            date=article.publication_date,
            header=article.header,
            source_link=article.url,
            arb_link=article.arb_link,
            arbitrage=article.arbitrage,
            form=article.form,
            streaming=article.streaming
        )
        return item

    # @staticmethod
    # def get_reads(string):
    #     pass
    # TODO нетрудно заметить, что нам нужно забирать число, даже если есть знак <

    @staticmethod
    def get_date(datestring):
        elements = datestring.lower().split("\xa0")
        final_date = datetime.datetime(1900, 12, 12, 12, 12, 12, 0)
        # datestring.lower().find('ago') == -1 and datestring.lower().find('day') == -1 and
        if datestring.lower().find('дня') == -1 and datestring.lower().find('чера') ==-1 and datestring.lower().find('назад') == -1:
            # yesterday, today, 3 days ago - всё тут)
            # months = ['january', 'february', 'march', 'april',
            #           'may', 'june', 'july', 'august',
            #           'september', 'october', 'november', 'december']
            months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября",
                      "ноября", "декабря"]
            month = months.index(elements[1]) + 1

            if len(elements) < 3:
                final_date = datetime.datetime(2020, month, int(elements[0]), 4, 20, 0, 0) # WARNING помни)
            else:
                final_date = datetime.datetime(int(elements[2]), month, int(elements[0]), 4, 20, 0, 0)
        # datestring.lower().find('today') != -1 or
        elif datestring.lower().find('егодня') != -1:  # TODO пофиксить отображение времени, эти 4.20 - такое себе
            final_date = datetime.datetime.now()
        # datestring.lower().find('yesterday') != -1 or
        elif datestring.lower().find('чера') != -1:
            tmp = datetime.datetime.now()
            final_date = datetime.datetime(tmp.year, tmp.month, tmp.day - 1, 4, 20, 0, 0)
        elif datestring.lower().find('назад') != -1:
            tmp = datetime.datetime.now()
            if elements[0] != "год":
                shift = int(elements[0])
                if elements[1] == 'дня' or elements[1] == 'дней':
                    final_date = datetime.datetime(tmp.year, tmp.month, tmp.day - shift)
                elif elements[1] == 'года' or elements[1] == 'лет':
                    final_date = datetime.datetime(tmp.year - shift, tmp.month, tmp.day)
                elif elements[1] == 'месяцев':
                    final_date = datetime.datetime(tmp.year, tmp.month - shift, tmp.day)
            else:
                final_date = datetime.datetime(tmp.year-1, tmp.month, tmp.day)

        return final_date
    
    def closed(self, reason):
        self.zen_conn.close()
    #   self.proxy_conn.close()

    # return True, если всё нормально. Иначе false

# TODO если есть плашка "партнерская статья" - не арбитражная)
#  Впрочем, это необязательно - мы несколько статей смотрим


class SecondLevelSpider(scrapy.Spider):
    name = "level2"

    allowed_domains = ["zen.yandex.ru", "zen.yandex.com"]
    # start_urls = ["https://zen.yandex.ru/media/zen/channels"]

    def start_requests(self):
        url = "https://zen.yandex.ru/media/zen/channels"
        yield scrapy.Request(url=url, callback=self.parse, dont_filter=False) # фильтрация магическим образом все ломает
        # и вот почему: сразу после process_exception мы кидаем запрос в scheduler
        # но он фильтрует... Так вот все и происходит) поэтому надо как-то это исправить, может с ретраем попытаться

    def parse(self, response):
        for a in response.css("div.alphabet__list a.alphabet__item::attr(href)").getall():
            if a != "media/zen/channels":
                self.logger.warning("Parsing letter: " + a)
                yield response.follow(a, callback=self.parse_by_letter, dont_filter=False)

    def parse_by_letter(self, response):
        channel_top = response.css("a.channel-item__link::attr(href)").get()
        if channel_top:
            # self.parse_from_page(response) # TODO не выполняется в принципе
            next_page = response.css("div.pagination-prev-next__button a.pagination-prev-next__link::attr(href)").getall()
            if len(next_page) > 1:
                # nxt_page = next_page[-1] TODO pay attention
                yield response.follow(next_page[-1], callback=self.parse_by_letter)

                chans = response.css("a.channel-item__link::attr(href)").getall()
                for chan in chans:
                    yield response.follow(chan, callback=self.parse_channel)

            elif len(next_page) == 1:
                # nxt_page = next_page[0] AND HERE
                yield response.follow(next_page[0], callback=self.parse_by_letter)

                chans = response.css("a.channel-item__link::attr(href)").getall()
                for chan in chans:
                    yield response.follow(chan, callback=self.parse_channel)

    def parse_channel(self, response):
        # chans = response.css("a.channel-item__link::attr(href)").getall()
        # for chan in chans:
        name = response.css("div.app-redesign-view__main-container div.desktop-channel-2-top__title::text").get()
        if name:
            self.logger.warning("Processing channel: " + name)
            item = ZencrawlersourceItem(channel_name=name, channel_url=response.url)
        else:
            self.logger.warning("Processing channel with blank name: " + response.url)
            item = ZencrawlersourceItem(channel_name="NoneThing", channel_url=response.url)
        yield item



class TestSpider(scrapy.Spider):
    name = "zentest"

    allowed_domains = ["zen.yandex.ru", "zen.yandex.com"]
    start_urls = ["https://zen.yandex.ru/media/zen/channels"]

    def parse(self, response):

        for a in tqdm(response.css("div.alphabet__list a.alphabet__item::attr(href)").getall()):
            if a != "media/zen/channels": # DONE теперь итерация правильная - TODO
                yield response.follow(a, callback=self.parse_by_letter)

    def parse_by_letter(self, response):
        channel_top = response.css("a.channel-item__link").get()
        while channel_top:  # DONE чекни, мб мы проебываем 1 страницу выдачи в каждой - TODO
            self.parse_from_page(response)
            next_page = response.css("div.pagination-prev-next__button a.pagination-prev-next__link::attr(href)").getall()[-1]
            yield response.follow(next_page, callback=self.parse_by_letter)

    def parse_from_page(self, response):
        chans = response.css("a.channel-item__link::attr(href)").getall()
        self.logger.warning(chans)
        with open('chans.txt', 'a') as f:
            f.write(chans)


class IPSpider(scrapy.Spider):
    name = "ips"

    # def __init__(self):
    #     self.proxy_conn = db_ops.connect_to_db("proxy_db", "postgres", "postgres", "127.0.0.1")

    start_urls = ["http://httpbin.org/ip"]

    def parse(self, response):
        print(response.text)

    # def closed(self, reason):
    #     self.proxy_conn.close()
    #     print("Closed connection with proxy_db")

    # def errback_httpbin(self, failure):

