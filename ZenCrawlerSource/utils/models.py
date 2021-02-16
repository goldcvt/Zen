import peewee
from datetime import datetime
from ZenCrawlerSource import settings

raw_db = peewee.PooledPostgresqlDatabase(
    *settings.DATABASE_CONNECTION_ARGS, **settings.DATABASE_CONNECTION_KWARGS,
)


class Proxy(peewee.Model):
    class Meta:
        database = raw_db
        db_table = "proxies"
        indexes = (
            (("raw_protocol", "auth_data", "domain", "port"), True),
            (("auth_data", "domain", "port"), False),  # important!
            (("raw_protocol",), False),
            (("auth_data",), False),
            (("domain",), False),
            (("port",), False),
            (("number_of_bad_checks",), False),
            (("next_check_time",), False),
            (("last_check_time",), False),
            (("checking_period",), False),
            (("uptime",), False),
            (("bad_uptime",), False),
            (("response_time",), False),
            (("_white_ipv4",), False),
            (("_white_ipv6",), False),
        )

    PROTOCOLS = (
        "http",
        "socks4",
        "socks5",
    )

    raw_protocol = peewee.SmallIntegerField(null=False)
    domain = peewee.CharField(settings.DB_MAX_DOMAIN_LENGTH, null=False)
    port = peewee.IntegerField(null=False)
    auth_data = peewee.CharField(settings.DB_AUTH_DATA_MAX_LENGTH, default="", null=False)

    checking_period = peewee.IntegerField(default=settings.MIN_PROXY_CHECKING_PERIOD, null=False)
    last_check_time = peewee.IntegerField(default=0, null=False)
    next_check_time = peewee.IntegerField(default=0, null=False)
    number_of_bad_checks = peewee.IntegerField(default=0, null=False)
    uptime = peewee.IntegerField(default=None, null=True)
    bad_uptime = peewee.IntegerField(default=None, null=True)
    # in microseconds
    response_time = peewee.IntegerField(default=None, null=True)
    # TODO: consider storing as binary
    _white_ipv4 = peewee.CharField(16, null=True)
    _white_ipv6 = peewee.CharField(45, null=True)

    def get_raw_protocol(self):
        return self.raw_protocol

    @property
    def address(self):
        return self.to_url()

    @property
    def protocol(self):
        return self.PROTOCOLS[int(self.raw_protocol)]

    @protocol.setter
    def protocol(self, protocol):
        self.raw_protocol = self.PROTOCOLS.index(protocol)

    @property
    def bad_proxy(self):
        return self.number_of_bad_checks > 0

    @property
    def white_ipv4(self):
        return self._white_ipv4

    @white_ipv4.setter
    def white_ipv4(self, value):
        self._white_ipv4 = value

    @property
    def white_ipv6(self):
        return self._white_ipv6

    @white_ipv6.setter
    def white_ipv6(self, value):
        self._white_ipv6 = value

    def to_url(self, protocol=None):
        address = protocol if protocol is not None else self.PROTOCOLS[int(self.raw_protocol)]
        address += "://"
        if self.auth_data:
            address += self.auth_data + "@"

        address += "{}:{}".format(self.domain, self.port)

        return address

    def __str__(self):
        return self.to_url()

    __repr__ = __str__


class BannedByYandexProxy(peewee.Model):
    class Meta:
        database = raw_db
        db_table = "banned_by_yandex"

    _proxy_id = peewee.ForeignKeyField(Proxy, backref="bannedbyyandex")
    _banned_at = peewee.DateTimeField(default=datetime.datetime.now(), null=True)
    last_check = peewee.DateTimeField(null=True)

    @property
    def proxy_id(self):
        return self._proxy_id

    @proxy_id.setter
    def proxy_id(self, value):
        self._proxy_id = value

    @property
    def banned_at(self):
        return self._banned_at

    @banned_at.setter
    def banned_at(self, value):
        self._banned_at = value
