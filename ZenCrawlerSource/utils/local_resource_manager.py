from ZenCrawlerSource.utils.models import Proxy, BannedByYandexProxy, raw_db
from psycopg2 import connect
from datetime import datetime
import socket
from peewee import JOIN


class DeleGatePortManager:

    '''
        Reserves and releases ports so no conflict can happen
    '''

    def __init__(self):
        self.used_ports = None

    def reserve_port(self, port):
        if self.used_ports is not None:
            self.used_ports.append(int(port))
        else:
            self.used_ports = []
            self.used_ports.append(int(port))

    def release_port(self, port):  # из request.meta достается номер порта при обработке исключения - когда умер прокси
        if self.used_ports is not None:
            self.used_ports.remove(int(port))
        else:
            pass

    @staticmethod
    def get_free_port():
        """
        Determines a free port using sockets.
        """
        free_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        free_socket.bind(('127.0.0.1', 0))
        free_socket.listen(5)
        port = free_socket.getsockname()[1]
        free_socket.close()
        return port


class NoProxiesError(BaseException):
    pass


class BadProxyException(BaseException):
    pass


class ProxyManager:
    '''
    SELECT * FROM proxies
    LEFT JOIN banned_by_yandex AS banned
        ON banned.__proxy_id = proxies.id
    WHERE banned.__proxy_id IS NULL
    '''

    @staticmethod
    def get_proxy(proto='http', bad_checks=0):

        if proto == 'http':
            proto_num = 0
        elif proto == 'socks4':
            proto_num = 1
        else:
            proto_num = 2

        banned = BannedByYandexProxy.alias()
        predicate = (banned._proxy_id == Proxy.id)
        proxy = Proxy.select().join(banned, JOIN.LEFT_OUTER, on=predicate).where(
            banned._proxy_id.is_null(True),
            Proxy.raw_protocol == proto_num,
            Proxy.number_of_bad_checks == bad_checks
        ).order_by(
            Proxy.last_check_time.desc()
        ).limit(1).get()
        while proxy.location["country_code"] == "RU":
            ProxyManager.blacklist_proxy(proxy.to_url(protocol=proto))
            proxy = Proxy.select().join(banned, JOIN.LEFT_OUTER, on=predicate).where(
                banned._proxy_id.is_null(True),
                Proxy.raw_protocol == proto_num,
                Proxy.number_of_bad_checks == bad_checks
            ).order_by(
                fn.Random()
            ).limit(1).get()
        if proxy:
            return proxy.to_url(protocol=proto), proxy.location["country_code"]
        else:
            raise NoProxiesError

    @staticmethod
    def get_fallback_proxy():
        banned = BannedByYandexProxy.alias()
        predicate = (banned._proxy_id == Proxy.id)
        proxy = Proxy.select().join(banned, JOIN.LEFT_OUTER, on=predicate).where(
            banned._proxy_id.is_null(True)
        ).order_by(Proxy.uptime).limit(1).get()
        while proxy.location['country_code'] == 'RU':  # TODO delete whole cycle after you add support for RU proxies
            proxy = Proxy.select().join(banned, JOIN.LEFT_OUTER, on=predicate).where(
                banned._proxy_id.is_null(True)
            ).order_by(fn.Random()).limit(1).get()
        return proxy.to_url()

    @staticmethod
    def blacklist_proxy(proxy_string):
        proxy_string = proxy_string.split("/")[-1].split(":")[0]
        proxies = Proxy.select().where(Proxy.domain == proxy_string)
        for proxy in proxies:
            BannedByYandexProxy.insert(_banned_at=datetime.now(), _proxy_id=proxy.id, last_check=None).execute()

    # WARNING: THIS ONE DOES NOT OPERATE
    @staticmethod
    def free_from_blacklist(proxy):
        # TODO MAKE IT WORK!
        proxy.delete_instance()
