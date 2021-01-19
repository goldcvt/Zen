import scrapy
from crawler_toolz import db_ops
import datetime
import re
from ZenCrawlerSource.items import ChannelItem, ArticleItem, GalleryItem, ZencrawlersourceItem
import json
from tqdm import tqdm

non_arbitrage = ['instagram.com', 'twitter.com', 'wikipedia.org', 'google.ru', 'vimeo', 'youtube', 'vk', "yandex.ru/news", "yandex.ru/images"]


class Galleries:
    def __init__(self, created_at, modified_at, header, url, views=-1, reads=-1, arb_link='', arbitrage=False,
                 zen_related=False, has_bad_text=False, had_bad_image=False, dark_post=False, native_ads=False):
        self.created_at = created_at
        self.modified_at = modified_at
        self.header = header
        self.url = url
        self.views = views
        self.reads = reads
        self.arb_link = arb_link
        self.arbitrage = arbitrage
        self.zen_related = zen_related
        self.has_bad_text = has_bad_text
        self.had_bad_image = had_bad_image
        self.dark_post = dark_post
        self.native_ads = native_ads
        # TODO darkPost, hasNativeAds support
        # self.using_direct = using_direct

    def get_static_stats(self, response):
        my_data = response.css("script#all-data::text").get().encode('utf-8').strip().decode()
        try:
            my_ind = my_data.index("window._data = ")
            my_ind_fin = my_data.index("window._uatraits =")
        except ValueError:
            my_ind = my_data.index("w._data = ")
            my_ind_fin = my_data.index("w._uatraits =")
        my_json = json.loads(my_data[my_data[my_ind:].index("{")+my_ind:my_data[:my_ind_fin].rfind(';')])
        # print(json.dumps(my_json, indent=4, sort_keys=True)) # - a tangible output
        try:
            self.header = my_json["publication"]["content"]["preview"]["title"]
        except:
            self.header = "error"
        self.header = self.header.replace("'", "")
        try:
            datestamp = datetime.date.fromtimestamp(int(int(my_json["publication"]["addTime"])/1000))
        except KeyError:
            datestamp = None
        try:
            mod_datestamp = datetime.date.fromtimestamp(int(int(my_json["publication"]["content"]["modTime"])/1000))
        except KeyError:
            mod_datestamp = None
        try:
            self.native_ads = my_json["publication"]["hasNativeAds"]
        except KeyError:
            pass
        try:
            self.dark_post = my_json["publication"]["darkPost"]
        except KeyError:
            pass
        search_scope = json.loads(my_json["publication"]["content"]["articleContent"]["contentState"])
        link = ""
        tmp = False
        for i in search_scope['items']:
            try:
                if i['has_bad_text']:
                    self.has_bad_text = True
                if i['had_bad_image']:
                    self.had_bad_image = True
                for j in i["rich_text"]["json"]:
                    if "attribs" in j.keys():
                        if "href" in j["attribs"].keys():
                            link = j["attribs"]["href"]
                            for i in non_arbitrage:  # проверка на ссылочный мусор
                                if (link).find(i) != -1:
                                    tmp = True
                                    link = ""
                                    break
            except KeyError:
                pass

        if_link = None or link

        if if_link and (not tmp):
            self.arbitrage = True
            self.arb_link = if_link.replace("'", "")
            if self.arb_link.find("zen.yandex.ru") != -1:
                self.zen_related = True
        self.created_at = datestamp
        self.modified_at = mod_datestamp


class Articles:
    def __init__(self, created_at, modified_at, header, url, views=-1, reads=-1, arb_link='', arbitrage=False,
                 streaming=False, form=False, zen_related=False, using_direct=False, has_bad_text=False,
                 had_bad_image=False, native_ads=False, dark_post=False):
        self.created_at = created_at
        self.modified_at = modified_at
        self.header = header
        self.url = url
        self.views = views
        self.reads = reads
        self.arb_link = arb_link
        self.arbitrage = arbitrage
        self.form = form
        self.streaming = streaming
        self.zen_related = zen_related
        self.using_direct = using_direct
        self.has_bad_text = has_bad_text
        self.had_bad_image = had_bad_image
        self.native_ads = native_ads
        self.dark_post = dark_post

    def __str__(self):
        return f'{str(vars(self))}'

    def __repr__(self):
        return f'{str(vars(self))}'

    def using_form(self, response):
        forms = response.css("div.yandex-forms-embed").get()
        streaming = response.css("div.yandex-efir-embed").get()
        y_music = response.css("div.yandex-music-embed").get()
        yt = response.css("div.youtube-embed").get()
        kino = response.css("div.kinopoisk-embed").get()
        y_direct = response.css("div.yandex-direct-embed").get()
        other_embeds = response.css("div.article-render__block_embed").getall() # bug fixed
        if y_direct:
            self.using_direct = True
        if len(other_embeds) == 1 and y_direct:
            other_embeds = None
        if streaming:
            self.streaming = True
        elif (not (y_music or kino or yt)) and (forms or other_embeds):
            self.form = True
            self.arbitrage = True

    def is_arbitrage(self, response): # checks straight-up links
        if_p = response.css("p.article-render__block a.article-link::attr(href)").get() or ""
        if_blockquote = response.css("blockquote.article-render__block a.article-link::attr(href)").get() or ""
        if_header = response.css("h2.article-render__block a.article-link::attr(href)").get() or response.css("h3.article-render__block a.article-link::attr(href)").get() or ""
        tmp = False

        for i in non_arbitrage: # проверка на ссылочный мусор
            if (if_p + if_blockquote + if_header).find(i) != -1:
                tmp = True
                break

        if if_p or if_blockquote or if_header and (not tmp):
            self.arbitrage = True
            # питонно пиздец)
            self.arb_link = if_p.replace("'", "") or if_blockquote.replace("'", "") or if_header.replace("'", "")
            if self.arb_link.find("zen.yandex.ru") != -1:
                self.zen_related = True
        self.using_form(response)


class Channels:

    def __init__(self, subs, audience, url, links=None, articles=None, arbitrage=False, form=False, is_crawled=False, streaming=False):
        self.subs = int(subs)
        self.audience = int(audience)
        self.url = url
        self.links = links or []
        self.articles = articles or []
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

# class JSSpider(scrapy.Spider): # splash проще в 100500 раз... Но и он не нужен))
#     name = "js_nightcrawler"
#
#     allowed_domains = ["zen.yandex.ru"]
#
#     def __init__(self, url=None, *args, **kwargs):
#         super(JSSpider, self).__init__(*args, **kwargs)
#         self.start_urls = [f'{url}']
#
#     def parse(self, response):
#         pass
#
#     # firing it:
#     # process = CrawlerProcess(settings = {})
#     # process.crawl(JSSpider, url)
#     # process.start()

class ExampleSpider(scrapy.Spider):
    name = "nightcrawler"

    allowed_domains = ["zen.yandex.ru", "zen.yandex.com"]
    start_urls = ["https://zen.yandex.ru/media/zen/channels"]

    def parse(self, response):
        for a in tqdm(response.css("div.alphabet__list a.alphabet__item::attr(href)").getall()):
            if a != "media/zen/channels": # DONE теперь итерация правильная
                yield response.follow(a, callback=self.parse_by_letter, dont_filter=True)

    def parse_by_letter(self, response):
        channel_top = response.css("a.channel-item__link").get()
        if channel_top: # DONE чекни, мб мы проебываем 1 страницу выдачи в каждой
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

    def parse_channel(self, response): # DONE перевели на классы
        self.logger.info("Channel name: " + response.css("div.zen-app div.channel-header-view-desktop__info-block h1 span::text").get())
        default_stats = response.css("div.zen-app div.channel-info-view__block div.channel-info-view__value::text").getall()
        stat_kword = response.css("div.zen-app div.channel-info-view__block div.channel-info-view__name::text").get()
        # DONE implemented PC UA
        if len(default_stats) == 2:
            audience = int("".join(("".join(default_stats[0].split("<"))).split(" ")))
            subs = int("".join(("".join(default_stats[1].split("<"))).split(" ")))
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

        # can move that line to top and make if statement, so we only get channels w/ articles to bd
        urls = response.css("div.card-wrapper__inner a.card-image-view__clickable::attr(href)").getall()[:5]
        # уже специфично для статей

        galls = response.css("div.card-wrapper__inner a.card-gallery-desktop-view__clickable::attr(href)").getall()[:5]

        # CHANGE x in [:x] for different MAX amount of articles/galleries to be fetched

        self.logger.info(f"Itemizing {chan}")
        my_item = self.itemize_channel(chan)
        yield my_item

        if urls:
            if urls[0].find("zen.yandex.ru"):   # мало ли, вдруг мы зашли на сайтовый канал
                yield response.follow(urls[0],
                                      callback=self.fetch_article,
                                      cb_kwargs=dict(other_pubs=urls[1:])
                                      )
        if galls:
            yield response.follow(galls[0],
                                  callback=self.fetch_gallery,
                                  cb_kwargs=dict(other_pubs=galls[1:])
                                  )

    def fetch_gallery(self, response, other_pubs=None):
        base_date = datetime.date(1900, 12, 12)
        title = ""
        pub_id = ''.join(response.url.split("?")[0].split('-')[-1])
        views_req_url = f"https://zen.yandex.ru/media-api/publication-view-stat?publicationId={pub_id}"
        gall = Galleries(base_date, base_date, title, response.url)
        gall.get_static_stats(response)
        try:
            yield response.follow(views_req_url, callback=self.get_reads,
                                  cb_kwargs=dict(publication=gall))
        except Exception:
            gall_item = self.itemize_gallery(gall)
            yield gall_item

        # TODO check here
        if other_pubs and gall.created_at > (datetime.date.today() - datetime.timedelta(days=10)):
            yield from response.follow_all(other_pubs,
                                     callback=self.fetch_gallery
                                     )

    def fetch_article(self, response, other_pubs=None):
        title = response.css("div#article__page-root h1.article__title::text").get().encode('utf-8').strip()
        if title:
            title = title.decode().replace("'", "")
        # d_str = response.css("footer.article__statistics span.article-stat__date::text").get()
        base_date = datetime.date(1900, 12, 12)

        article = Articles(base_date, base_date, title, response.url)
        article.is_arbitrage(response)
        ExampleSpider.get_date(article, response)
        pub_id = ''.join(response.url.split("?")[0].split('-')[-1])
        views_req_url = f"https://zen.yandex.ru/media-api/publication-view-stat?publicationId={pub_id}"

        try:  # вот такое чувство, что все ломается именно здесь - потому и не было явных ошибок
            yield response.follow(views_req_url, callback=self.get_reads, cb_kwargs=dict(publication=article))
        except Exception:
            art_item = self.itemize_article(article)
            yield art_item

        #TODO pay attention
        if other_pubs and article.created_at > (datetime.date.today() - datetime.timedelta(days=10)):
            yield from response.follow_all(other_pubs,
                                     callback=self.fetch_article
                                     )

    def get_reads(self, response, publication):
        reads = -1
        views = -1
        resp_string = response.text
        try:
            my_dict = json.loads(resp_string)
            if my_dict:
                reads = my_dict["viewsTillEnd"]
                views = my_dict["views"]
        except json.decoder.JSONDecodeError:
            self.logger.warning("Error in getting reads due to json.decoder.JSONDecodeError")
        except NameError:
            self.logger.warning("Error in getting reads due to NameError")
        publication.reads = reads
        publication.views = views
        if isinstance(publication, Articles):
            item = self.itemize_article(publication)
        else:
            item = self.itemize_gallery(publication)
        yield item


    @staticmethod
    def itemize_channel(channel):
        item = ChannelItem(
                                subs=channel.subs,
                                audience=channel.audience,
                                url=channel.url,
                                contacts=channel.links, # может вызывать проблемы
                                last_checked=datetime.datetime.now()
        )
        return item

    @staticmethod
    def itemize_article(article):
        item = ArticleItem(
            created_at=article.created_at,
            modified_at=article.modified_at,
            header=article.header,
            url=article.url,
            views=article.views,
            reads=article.reads,
            arb_link=article.arb_link,
            arbitrage=article.arbitrage,
            form=article.form,
            streaming=article.streaming,
            zen_related=article.zen_related,
            has_bad_text=article.has_bad_text,
            had_bad_image=article.had_bad_image,
            native_ads=article.native_ads,
            dark_post=article.native_ads
        )
        return item

    @staticmethod
    def itemize_gallery(gallery):
        item = GalleryItem(
            created_at=gallery.created_at,
            modified_at=gallery.modified_at,
            header=gallery.header,
            url=gallery.url,
            views=gallery.views,
            reads=gallery.reads,
            arb_link=gallery.arb_link,
            arbitrage=gallery.arbitrage,
            zen_related=gallery.zen_related,
            has_bad_text=gallery.has_bad_text,
            had_bad_image=gallery.had_bad_image,
            native_ads=gallery.native_ads,
            dark_post=gallery.native_ads
        )
        return item

    @staticmethod
    def get_date(publication, response):
        try:
            my_data = response.css("script#all-data::text").get().encode('utf-8').strip().decode()
            try:
                my_ind = my_data.index("window._data = ")
                my_ind_fin = my_data.index("window._uatraits =")
            except ValueError:
                my_ind = my_data.index("w._data = ")
                my_ind_fin = my_data.index("w._uatraits =")
            my_json = json.loads(my_data[my_data[my_ind:].index("{") + my_ind:my_data[:my_ind_fin].rfind(';')])
            datestamp = datetime.date.fromtimestamp(int(int(my_json["publication"]["addTime"]) / 1000))
            publication.created_at = datestamp
            mod_datestamp = datetime.date.fromtimestamp(int(int(my_json["publication"]["content"]["modTime"]) / 1000))
            publication.modified_at = mod_datestamp

        except Exception:
            d_str = response.css("footer.article__statistics span.article-stat__date::text").get()
            publication.created_at = ExampleSpider.get_date_old(d_str)
            publication.modified_at = ExampleSpider.get_date_old(d_str)
            del d_str

        else:
            try:
                publication.native_ads = my_json["publication"]["hasNativeAds"]
            except KeyError:
                pass
            try:
                publication.dark_post = my_json["publication"]["darkPost"]
            except KeyError:
                pass
            search_scope = json.loads(my_json["publication"]["content"]["articleContent"]["contentState"])
            for i in search_scope['items']:
                try:
                    if i['has_bad_text']:
                        publication.has_bad_text = True
                    if i['had_bad_image']:
                        publication.had_bad_image = True
                except Exception:
                    pass

    @staticmethod
    def get_date_old(datestring):
        elements = datestring.lower().split("\xa0")
        final_date = datetime.datetime(1900, 12, 12)
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
        self.logger.warning("All done, master")
    #   self.zen_conn.close()
    #   self.proxy_conn.close()

    # return True, если всё нормально. Иначе false
