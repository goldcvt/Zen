from crawler_tools import db_ops
import psycopg2

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ZencrawlersourcePipeline:
    def process_item(self, item, spider):
        return item

# TODO написать исправлялку, если соединение с БД наебнется

class ChannelPipeline:

    def __init__(self):
        self.zen_conn = db_ops.connect_to_db("zen_copy", "obama", "obama", "127.0.0.1")

    def process_item(self, channel_item, spider):

        channel_dict = channel_item
        del channel_dict["articles"]
        del channel_dict["is_crawled"]

        if channel_item["is_crawled"]:
            # doing necessary stuff, you know
            conn = self.zen_conn
            cursor = conn.cursor()

            # updating articles
            for article in channel_item["articles"]:
                article_dict = dict(vars(article))
                request = "UPDATE articles SET"
                for key in article_dict.keys():
                    request += " {} = {}".format(key, article_dict[key])
                request += " WHERE channel_id = {};".format(channel_item["is_crawled"])
                cursor.execute(request)
                conn.commit()

            #updating channel
            request = "UPDATE channels SET"
            for key in channel_dict.keys():
                request += " {} = {}".format(key,channel_dict[key])
            request += " WHERE url = {}".format(channel_item["url"])

            cursor.execute(request)
            conn.commit()
            cursor.close()

        else:
            db_ops.write_to_db(self.zen_conn, "channels", **channel_dict)  # write_to_db (channel)
            channel_id = db_ops.read_from_db(self.zen_conn, "channels", "channel_id", where="url={}".format(
                    channel_item["url"]))[0][0]

            # put articles into db
            for article in channel_item["articles"]:
                article_dict = dict(vars(article), channel_id=channel_id)
                db_ops.write_to_db(self.zen_conn, "articles", **article_dict)  # TODO write_to_db - article

        del channel_dict
        del article_dict
        return channel_item  # TODO CHANGE TO DELETION