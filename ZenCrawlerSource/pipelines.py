from crawler_toolz import db_ops
import datetime
from psycopg2 import InterfaceError
from scrapy import signals
from ZenCrawlerSource.items import ChannelItem


# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ZencrawlersourcePipeline:
    def process_item(self, item, spider):
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
        crawler.signals.connect(s.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(s.close_spider, signal=signals.spider_closed)
        return s

    def open_spider(self, spider):
        self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
        spider.logger.info(f"Opened connection with zen_copy: {self.conn}")

    def close_spider(self, spider):
        self.conn.close()
        spider.logger.info(f"Closed connection with zen_copy: {self.conn}")

    def process_item(self, item, spider):
        # if isinstance(item, ChannelItem):  # i'm probably paranoid
        try:
            spider.logger.info("ITEM IS IN PIPELINE, PORCESSING...")
            channel_dict = item
            del channel_dict["articles"]
            del channel_dict["is_crawled"]

            if item["is_crawled"]:
                item["last_checked"] = datetime.datetime.now()
                # doing necessary stuff, you know
                conn = self.conn
                cursor = conn.cursor()

                # updating articles
                for article in item["articles"]:
                    article_dict = dict(vars(article))
                    request = "UPDATE articles SET"
                    for key in article_dict.keys():
                        request += " {} = {}".format(key, article_dict[key])
                    request += " WHERE channel_id = {};".format(item["is_crawled"])
                    cursor.execute(request)
                    conn.commit()

                #updating channel
                request = "UPDATE channels SET"
                for key in channel_dict.keys():
                    request += " {} = {}".format(key, channel_dict[key])
                request += " WHERE url = {}".format(item["url"])

                cursor.execute(request)
                conn.commit()
                cursor.close()

            else:
                db_ops.write_to_db(self.conn, "channels", **channel_dict)  # write_to_db (channel)
                channel_id = db_ops.read_from_db(self.conn, "channels", "channel_id", where="url={}".format(
                        item["url"]))[0][0]

                # put articles into db
                for article in item["articles"]:
                    article_dict = dict(vars(article), channel_id=channel_id)
                    db_ops.write_to_db(self.conn, "articles", **article_dict)  # TODO write_to_db - article

            spider.logger.info("ITEM PROCESSED")

            # if channel_dict:
            #     spider.logger.info(channel_dict)
            # if article_dict:
            #     print(article_dict)
            return item  # TODO CHANGE TO DELETION?

        except InterfaceError:
            spider.logger.info("ITEM DB CONN FAILED, RE-ESTABLISHING")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            self.process_item(item, spider)
        except AttributeError:
            spider.logger.info("ITEM DB CONN FAILED, RE-ESTABLISHING")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            self.process_item(item, spider)
        # else:
        #     return item
