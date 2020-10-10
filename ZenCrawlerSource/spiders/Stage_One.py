import scrapy
from crawler_toolz import db_ops
import datetime
import re
from ZenCrawlerSource.items import ArticleItem, ChannelItem
import json
from tqdm import tqdm
from psycopg2 import InterfaceError

# подавляет тупые строчки с device type incompatible
import logging
logging.getLogger("scrapy-user-agents").setLevel(logging.DEBUG)

non_arbitrage = ['instagram.com', 'twitter.com']

# class Article():
#     def __int__(self, channel_id, reads, date, header, source_link, outer_link=None, text=None, arbitrage=False):
#         self.channel_id = channel_id
#         self.reads = int(reads)
#         self.date = date
#         self.header = header
#         self.source_link = source_link
#
#         self.outer_link = outer_link
#         self.arbitrage = True


class Articles():
    def __int__(self, date, header, url, arb_link=None, arbitrage=False, form=False):
        # TODO how to get that sweet link? Got it, just read and then add when creating
        self.publication_date = date
        self.header = header
        self.url = url
        self.arb_link = arb_link
        self.arbitrage = arbitrage
        self.form = form

    # def __iter__(self): DEPRECATED
    #     for attr, value in self.__dict__.iteritems():
    #         yield attr, value

    def using_form(self, response):
        forms = response.css("div.yandex-forms-embed").get()
        other_embeds = response.css("div.article-render__block.article-render__block_embed").get()
        if forms or other_embeds:
            self.form = True
            self.arbitrage = True

    def is_arbitrage(self, response):
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
    arbitrage: bool

    def __init__(self, subs, audience, url, links=[], articles=[], form=False, is_crawled=False):
        self.subs = int(subs)
        self.audience = int(audience)
        self.url = url
        self.links = links
        self.articles = articles
        self.form = form
        self.is_crawled = is_crawled

    @staticmethod
    def parse_description(response):
        desc_links = response.css("div.desktop-channel-2-description a::attr(href)").getall()
        desc_text = response.css("div.desktop-channel-2-description::text").get()
        emails = re.findall("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", desc_text)
        if desc_links or emails:
            return desc_links + emails
        else:
            return []

    def get_contacts(self, response):
        contacts = response.css("div.desktop-channel-2-social-links a.desktop-channel-2-social-links__item::attr(href)").getall()
        contacts += Channels.parse_description(response)
        if contacts:
            self.links = contacts

    def is_arbitrage(self, number_of_articles):
        # if len(self.articles) == 5: - DEPRECATED
        i = 0
        j = 0
        for article in self.articles:
            if article.arbitrage:
                i += 1
            if article.form:
                j += 1

        if i / number_of_articles >= 0.5:
            self.arbitrage = True
        else:
            self.arbitrage = False

        if j >= 1:
            self.form = True
        return self

    def if_crawled(self, conn): # чекаем, что уже есть в нашей дб) тогда тащем-та столбец my не имеет смысла
        found = db_ops.read_from_db(conn, "channels", "channel_id", where="url={}".format(self.url))[0][0]
        # DEBUG а мы что возвращаем?)
        if not found:
            self.is_crawled = False
        else:
            self.is_crawled = found


class ExampleSpider(scrapy.Spider):
    name = "nightcrawler"

    allowed_domains = ["zen.yandex.ru", "zen.yandex.com"]
    start_urls = ["https://zen.yandex.ru/media/zen/channels"]

    def __init__(self):
        # self.proxy_conn = db_ops.connect_to_db("proxy_db", "postgres", "postgres", "127.0.0.1")
        self.zen_conn = db_ops.connect_to_db("zen_copy", "obama", "obama", "127.0.0.1")

    def parse(self, response):

        for a in tqdm(response.css("div.alphabet__list a.alphabet__item::attr(href)").getall()):
            if a != "media/zen/channels": # DONE теперь итерация правильная - TODO
                yield response.follow(a, callback=self.parse_by_letter)

    def parse_by_letter(self, response):
        channel_top = response.css("a.channel-item__link").get()
        while channel_top: # DONE чекни, мб мы проебываем 1 страницу выдачи в каждой - TODO
            self.parse_from_page(response)
            next_page = response.css("div.pagination-prev-next__button a.pagination-prev-next__link::attr(href)").getall()[-1]
            yield response.follow(next_page, callback=self.parse_by_letter)

    def parse_from_page(self, response):
        chans = response.css("a.channel-item__link::attr(href)").getall()
        yield from response.follow_all(chans, callback=self.parse_channel) # just calls, no returns

    def parse_channel(self, response): # DONE перевели на классы - TODO
        default_stats = response.css("div.desktop-channel-2-counter__value::text").getall()
        # DONE implemented PC UA TODO
        subs = int("".join(default_stats[0].get().split(" ")))
        audience = int("".join(default_stats[1].get().split(" ")))
        # DONE return those! Items and item pipelines TODO
        chan = Channels(subs, audience, response.url)
        chan.get_contacts(response)
        try:
            chan.if_crawled(self.zen_conn)
        except InterfaceError:
            self.zen_conn = db_ops.connect_to_db("zen_copy", "obama", "obama", "127.0.0.1")
            chan.if_crawled(self.zen_conn)
        urls = response.css("div.card-wrapper__inner a::attr(href)").getall()[:5]
        # CHANGE x in [:x] for different amount of articles to be fetched

        for url in urls:
            if url.find("zen.yandex.ru"):   # мало ли, вдруг мы зашли на сайтовый канал
                yield response.follow(url,
                                      callback=self.fetch_article,
                                      cb_kwargs=dict(channel=chan, total_articles=len(urls))
                                      )
        del chan

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

    def fetch_article(self, response, channel, total_articles):
        title = response.css("h1.article__title::text").get()

        date = ExampleSpider.get_date(response.css("footer.article__statistics span.article-stat__date::text")
                                      .get())
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
        channel.articles.append(article)

        if len(channel.articles) == total_articles:
            channel.is_arbitrage(total_articles)
            yield ExampleSpider.itemize(channel)
        # raise scrapy.exceptions.CloseSpider(reason='Test completed') TODO implement constraints
        # TODO Fix this в целом плохой перевод в айтемы, ведь по сути у нас уже есть объекты нужные

    def get_reads(self, response):
        resp_string = u"{}".format(response.css("body p").get())
        my_dict = json.loads(resp_string)
        reads = my_dict["viewsTillEnd"]
        views = my_dict["views"]
        return reads, views


    @staticmethod
    def itemize(channel):
        # articles = [ExampleSpider.article_to_item(article) for article in channel.articles]
        chan_item = ChannelItem(
                                channel.subs,
                                channel.audience,
                                channel.url,
                                channel.links,
                                channel.articles,
                                channel.arbitrage,
                                channel.form,
                                channel.is_cralwed
                                )
        return chan_item

    # @staticmethod
    # def get_reads(string):
    #     pass
    # TODO нетрудно заметить, что нам нужно забирать число, даже если есть знак <

    @staticmethod
    def get_date(datestring):
        elements = datestring.lower().split(" ")
        final_date = datetime.datetime(1900, 12, 12, 12, 12, 12, 0)
        if datestring.lower().find('ago') == -1 and datestring.lower().find('day') == -1:
            # yesterday, today, 3 days ago - всё тут)
            months = ['january', 'february', 'march', 'april',
                      'may', 'june', 'july', 'august',
                      'september', 'october', 'november', 'december']
            month = months.index(elements[1]) + 1

            if len(elements) < 3:
                final_date = datetime.datetime(2020, month, int(elements[0]), 4, 20, 0, 0) # WARNING помни)
            else:
                final_date = datetime.datetime(int(elements[2]), month, int(elements[0]), 4, 20, 0, 0)

        elif datestring.lower().find('today') != -1:  # TODO пофиксить отображение времени, эти 4.20 - такое себе
            final_date = datetime.datetime.now()
        elif datestring.lower().find('yesterday') != -1:
            tmp = datetime.datetime.now()
            final_date = datetime.datetime(tmp.year, tmp.month, tmp.day - 1, 4, 20, 0, 0)
        elif datestring.lower().find('ago') != -1:
            tmp = datetime.datetime.now()
            shift = int(datestring.split(" ")[0])
            final_date = datetime.datetime(tmp.year, tmp.month, tmp.day - shift)
        return final_date

    # если уже есть, просто удаляем из БД (а лучше ALTER)
    
    def closed(self, reason):
        self.zen_conn.close()
    #   self.proxy_conn.close()

    # return True, если всё нормально. Иначе false

# TODO если есть плашка "партнерская статья" - не арбитражная)
#  Впрочем, это необязательно - мы несколько статей смотрим

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

