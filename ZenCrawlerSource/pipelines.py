from crawler_toolz import db_ops
import datetime
from psycopg2 import InterfaceError
from psycopg2.errors import SyntaxError
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
        # doing necessary stuff, you know
        try:
            if isinstance(item, ChannelItem):
                item["url"] = item["url"].split("?")[0]
                test = db_ops.read_from_db(self.conn, "channels", "channel_id", where="url=\'{}\'".format(item["url"]))
                cursor = self.conn.cursor()
                spider.logger.info("CHANNEL ITEM IS IN PIPELINE, PROCESSING...")
                if test:
                    #updating channel
                    request = "UPDATE channels SET ("
                    for key in item.keys():
                        if isinstance(item[key], str):
                            request += " {} = \'{}\',".format(key, item[key])
                        else:
                            request += " {} = {},".format(key, item[key])
                        request = request[:-1] + ")"
                    request += " WHERE url = \'{}\';".format(item["url"])
                else:
                    valz = ""
                    keyz = ""
                    for key in item.keys():
                        keyz += "{}, ".format(key)
                        if isinstance(item[key], str):
                            valz += "\'{}\', ".format(item[key])
                        else:
                            valz += "{}, ".format(item[key])
                    keyz = keyz[:-2]
                    valz = valz[:-2]
                    request = "INSERT INTO channels ({}) VALUES ({});".format(keyz, valz)

                cursor.execute(request)
                self.conn.commit()

                spider.logger.info("CHANNEL ITEM PROCESSED")

            elif isinstance(item, ArticleItem):
                item["url"] = item["url"].split("?")[0]
                test = db_ops.read_from_db(self.conn, "articles", "url", where="url=\'{}\'".format(item["url"]))

                spider.logger.info("ARTICLE ITEM IS IN PIPELINE, PROCESSING...")
                channel_url_array = item["url"].split("/")
                if 'id' in channel_url_array:
                    channel_url = 'https://zen.yandex.ru/id/' + channel_url_array[-2]
                else:
                    channel_url = 'https://zen.yandex.ru/' + channel_url_array[-2]

                channel_id = db_ops.read_from_db(self.conn, "channels", "channel_id", where="url=\'{}\'".format(channel_url))
                if channel_id:
                    channel_id = channel_id[0][0]
                cursor = self.conn.cursor()

                if channel_id and test:
                    request = "UPDATE articles SET (channel_url = \'{}\',".format(channel_url)
                    for key in item.keys():
                        if isinstance(item[key], str):
                            request += " {} = \'{}\',".format(key, item[key])
                        else:
                            request += " {} = {},".format(key, item[key])
                    request = request[:-1] + ')'
                    request += " WHERE channel_id = {};".format(channel_id)

                elif channel_id and not test: # didn't write previously, but at this launch crawler got channel written to db
                    # so we have channel_id
                    request = "UPDATE articles SET channel_id = {} WHERE channel_url = \'{}\';".format(channel_id, channel_url)
                    # update articles from this launch, set chan_id for them

                    cursor.execute(request)
                    self.conn.commit()

                    valz = "{}, {}, ".format(channel_id, channel_url)
                    keyz = "channel_id, channel_url, "
                    for key in item.keys():
                        keyz += "{}, ".format(key)
                        if isinstance(item[key], str):
                            valz += "\'{}\', ".format(item[key])
                        else:
                            valz += "{}, ".format(item[key])
                    keyz = keyz[:-2]
                    valz = valz[:-2]
                    request = "INSERT INTO articles ({}) VALUES ({});".format(keyz, valz)

                else: # know only url
                    valz = "{}, ".format(channel_url)
                    keyz = "channel_url, "
                    for key in item.keys():
                        keyz += "{}, ".format(key)
                        if isinstance(item[key], str):
                            valz += "\'{}\', ".format(item[key])
                        else:
                            valz += "{}, ".format(item[key])
                    keyz = keyz[:-2]
                    valz = valz[:-2]
                    request = "INSERT INTO articles ({}) VALUES ({});".format(keyz, valz)

                cursor.execute(request)
                self.conn.commit()
            cursor.close()
            spider.logger.info("ARTICLE ITEM PROCESSED")

        except InterfaceError:
            spider.logger.info("ITEM DB CONN FAILED, RE-ESTABLISHING")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            self.process_item(item, spider)
        except AttributeError:
            spider.logger.info("ITEM DB CONN FAILED, RE-ESTABLISHING")
            self.conn = db_ops.connect_to_db(self.db, self.usr, self.pswd, self.hst)
            self.process_item(item, spider)
        # except SyntaxError:
        #     self.conn.rollback()


        return item  # TODO CHANGE TO DELETION?

