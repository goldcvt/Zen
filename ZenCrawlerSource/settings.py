# Scrapy settings for ZenCrawlerSource project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
DATABASE_CONNECTION_ARGS = ()
DATABASE_CONNECTION_KWARGS = {
    'host': '146.185.242.87',
    'database': 'proxy_py',
    'user': 'proxy_py',
    'password': 'proxy_py',
}
DB_MAX_DOMAIN_LENGTH = 128
DB_AUTH_DATA_MAX_LENGTH = 64
MIN_PROXY_CHECKING_PERIOD = 15 * 60
BOT_NAME = 'ZenCrawlerSource'

SPIDER_MODULES = ['ZenCrawlerSource.spiders']
NEWSPIDER_MODULE = 'ZenCrawlerSource.spiders'
GEOLITE2_CITY_FILE_LOCATION = '/home/ubuntu/GeoLite2-City_20210223/GeoLite2-City.mmdb'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'ZenCrawlerSource (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

#Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 5

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
CONCURRENT_REQUESTS_PER_IP = 5

# Disable cookies (enabled by default)
COOKIES_ENABLED = False  # just in case, y'know

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'ru-RU,ru;q=0.9',
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    # 'ZenCrawlerSource.middlewares.ZencrawlersourceSpiderMiddleware': 543,
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 100
}
REDIRECT_MAX_TIMES = 5
# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    # 'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 420,
    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,
    # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 400, # TODO check if commenting it breaks everything
    # 'ZenCrawlerSource.middlewares.IPNoRetryDownloaderMiddleware': 120,
    # 'ZenCrawlerSource.middlewares.IPTestDownloaderMiddleware': 120,
    # 'ZenCrawlerSource.middlewares.ZencrawlersourceDownloaderMiddleware': 120,
    # 'ZenCrawlerSource.middlewares.FortyGrandRequestsMiddleware': 120,
    'ZenCrawlerSource.middlewares.LatestDownloaderMiddleware': 120,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 410,
    # 'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    # 'scrapy_fake_useragent.middleware.RetryUserAgentMiddleware': 401
}

# LOG_FILE = "logs.txt"
RANDOM_UA_PER_PROXY = True
RANDOM_UA_TYPE = 'desktop.random'

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': 600,
    'scrapy.extensions.closespider.CloseSpider': 500,
    'scrapy.extensions.memdebug.MemoryDebugger': 300
}

DUPEFILTER_DEBUG = False

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'ZenCrawlerSource.pipelines.ZencrawlersourcePipeline': 300,
    'ZenCrawlerSource.pipelines.ChannelPipeline': 150,
    # 'ZenCrawlerSource.pipelines.EmptyPipeline': 150,
    # 'ZenCrawlerSource.pipelines.SpeedTestPipeline': 150
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 0.5
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 5
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 12
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = True

HTTPERROR_ALLOWED_CODES = []  # actually, spider doesn't get non-200 responses to parse, downloader still works tho

# Memory Debugger Ext SETTINGS
MEMDEBUG_ENABLED = False

RETRY_ENABLED = True

# CloseSpider Ext SETTINGS - ANCHOR
# Кстати, очевидно, что если мы закроем паучару, то соединения тоже закроются и нихуя мы уже не запишем, если оно в
# пайплайне
# CLOSESPIDER_ITEMCOUNT = 10
# CLOSESPIDER_TIMEOUT = 720000

TELNETCONSOLE_USERNAME = 'goldcat'
TELNETCONSOLE_PASSWORD = 'scrapes'

DOWNLOAD_TIMEOUT = 15

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
