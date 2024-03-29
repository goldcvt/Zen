from crawler_toolz import db_ops as do
import psycopg2

conn = psycopg2.connect("zen_copy", "", "", "127.0.0.1")

my_curs = conn.cursor()
articles_to_fix = my_curs.execute("SELECT DISTINCT channel_url FROM articles WHERE channel_id=''").fetchall()
my_curs.close()

curs = conn.cursor()
for article in articles_to_fix:
    try:
        c_url = article[0]
        chan_id = do.read_from_db(conn, "channels", "channel_id", where="url = {}".format(c_url))[0][0]
        request = "UPDATE articles SET channel_id = {} WHERE channel_url = {} AND channel_id = ''".format(chan_id, c_url)
        curs.execute()
        conn.commit()
    except Exception:
        conn.rollback()
        curs.close()
        conn.close()
        print("We've fucked up")
        raise AssertionError
curs.close()

# TODO ADD arbitrage_checker for channels if needed
# new_curs = conn.cursor()
# new_curs.close()
conn.close()
