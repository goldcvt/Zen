# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ZencrawlersourceItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

# item version of article and channel


class ArticleItem(scrapy.Item):
    # channel_id = scrapy.Field()
    date = scrapy.Field()
    header = scrapy.Field()
    source_link = scrapy.Field()
    arb_link = scrapy.Field()
    arbitrage = scrapy.Field()
    form = scrapy.Field()


class ChannelItem(scrapy.Item):
    subs = scrapy.Field()
    audience = scrapy.Field()
    url = scrapy.Field()
    contacts = scrapy.Field()
    articles = scrapy.Field()
    is_arbitrage = scrapy.Field()
    form = scrapy.Field()
    is_crawled = scrapy.Field()
    last_checked = scrapy.Field()



