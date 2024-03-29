# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ZencrawlersourceItem(scrapy.Item):
    # define the fields for your item here like:
    channel_name = scrapy.Field()
    channel_url = scrapy.Field()

# item version of article and channel

# TODO перевести на created_at/modified_at, думаю так пополезнее будет
class ArticleItem(scrapy.Item):
    created_at = scrapy.Field()
    modified_at = scrapy.Field()
    # date = scrapy.Field() # deprecated
    header = scrapy.Field()
    url = scrapy.Field()
    views = scrapy.Field()
    reads = scrapy.Field()
    arb_link = scrapy.Field()
    arbitrage = scrapy.Field()
    form = scrapy.Field()
    streaming = scrapy.Field()
    zen_related = scrapy.Field()
    using_direct = scrapy.Field()
    has_bad_text = scrapy.Field()
    native_ads = scrapy.Field()
    dark_post = scrapy.Field()


class ChannelItem(scrapy.Item):
    subs = scrapy.Field()
    audience = scrapy.Field()
    url = scrapy.Field()
    contacts = scrapy.Field()
    # is_arbitrage = scrapy.Field()
    # form = scrapy.Field()
    # whether_crawled = scrapy.Field()
    last_checked = scrapy.Field()
    # is_streaming = scrapy.Field()
    # streaming_since = scrapy.Field()
    # arbitrage_since = scrapy.Field()


class GalleryItem(scrapy.Item):
    created_at = scrapy.Field()
    modified_at = scrapy.Field()
    header = scrapy.Field()
    url = scrapy.Field()
    views = scrapy.Field()
    reads = scrapy.Field()
    arb_link = scrapy.Field()
    arbitrage = scrapy.Field()
    zen_related = scrapy.Field()
    using_direct = scrapy.Field()
    has_bad_text = scrapy.Field()
    native_ads = scrapy.Field()
    dark_post = scrapy.Field()

