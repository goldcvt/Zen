from crawler_toolz import db_ops
import datetime
from psycopg2 import InterfaceError
from scrapy import signals
from ZenCrawlerSource.items import ChannelItem, ArticleItem


# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ZencrawlersourcePipeline:
    def process_item(self, item, spider):
        with open("channels.txt", "a+") as f:
            f.write(str(vars(item)))
        return item

# DONE написать исправлялку, если соединение с БД наебнется


class ChannelPipeline:

    def __init__(self):
        self.db = "zen_copy"
        self.usr = "obama"
        self.pswd = "obama"
        self.hst = "127.0.0.1"
        self.conn = None

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()  # also calls __init__
        # crawler.signals.connect(s.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(s.close_spider, signal=signals.spider_closed)
        return s

    def open_spider(self, spider):
        self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
        spider.logger.info(f"Opened connection with zen_copy: {self.conn}")

    def close_spider(self, spider):
        self.conn.close()
        spider.logger.info(f"Closed connection with zen_copy: {self.conn}")

    def process_item(self, item, spider):
        if isinstance(item, ChannelItem):  # i'm probably paranoid
            try:
                spider.logger.info("CHANNEL ITEM IS IN PIPELINE, PROCESSING...")
                if item["is_crawled"]:
                    # doing necessary stuff, you know
                    conn = self.conn
                    cursor = conn.cursor()

                    #updating channel
                    request = "UPDATE channels SET"
                    for key in item.keys():
                        request += " {} = {}".format(key, item[key])
                    request += " WHERE url = {}".format(item["url"])

                    cursor.execute(request)
                    conn.commit()
                    cursor.close()

                else:
                    db_ops.write_to_db(self.conn, "channels", **item)  # write_to_db (channel)

                spider.logger.info("CHANNEL ITEM PROCESSED")

            except InterfaceError:
                spider.logger.info("ITEM DB CONN FAILED, RE-ESTABLISHING")
                self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
                self.process_item(item, spider)
            except AttributeError:
                spider.logger.info("ITEM DB CONN FAILED, RE-ESTABLISHING")
                self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
                self.process_item(item, spider)

        elif isinstance(item, ArticleItem):
            conn = self.conn
            cursor = conn.cursor()
            channel_url_array = (item["source_link"].split("?")[0]).split("/")
            if 'id' in channel_url_array:
                channel_url = 'https://zen.yandex.ru/id/' + channel_url_array[-2]
            else:
                channel_url = 'https://zen.yandex.ru/' + channel_url_array[-2]
            channel_id = db_ops.read_from_db(conn, "channels", "channel_id", where="url={}".format(channel_url))[0][0]
            quantity = len(db_ops.read_from_db(conn, "articles", "id", where="channel_url={}".format(channel_url)))

            if channel_id and quantity == 5:
                request = "UPDATE articles SET"
                for key in item.keys():
                    request += " {} = {}".format(key, item[key])
                request += " WHERE channel_id = {};".format(channel_id)

                cursor.execute(request)
                conn.commit()
                cursor.close()
            elif channel_id:
                db_ops.write_to_db(self.conn, "articles", **item, channel_id=channel_id)

                # we probably don't need stuff below
                # request = "UPDATE articles SET channel_id = {} WHERE channel_url = {}".format(channel_id, channel_url)
                #
                # cursor.execute(request)
                # conn.commit()
                # cursor.close()
            else:
                db_ops.write_to_db(self.conn, "articles", **item, channel_url=channel_url)


        return item  # TODO CHANGE TO DELETION?

