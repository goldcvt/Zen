from ZenCrawlerSource.utils.models import Proxy, BannedByYandexProxy, raw_db
from psycopg2 import connect
from datetime import datetime
import socket


class DeleGatePortManager:

    '''
        Reserves and releases ports so no conflict can happen
    '''

    def __init__(self):
        self.used_ports = None

    def reserve_port(self, port):
        if self.used_ports is not None:
            self.used_ports.append(port)
        else:
            self.used_ports = []
            self.used_ports.append(port)

    def release_port(self, port):  # из request.meta достается номер порта при обработке исключения - когда умер прокси
        if self.used_ports is not None:
            self.used_ports.remove(port)
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
    @staticmethod
    def get_proxy(proto='http', bad_checks=0):
        proxy = Proxy.select().where(Proxy.protocol == proto, Proxy.number_of_bad_checks == bad_checks).order_by(
            Proxy.last_check_time.desc()).limit(1)  # TODO add condition for proxy being banned by yandex
        # TODO add condition blocking RU proxy (or ability to choose proxy geo)
        if proxy:
            proxy = proxy.to_url(protocol=proto)
            return proxy
        else:
            raise NoProxiesError

    @staticmethod
    def get_fallback_proxy():
        proxy = Proxy.select().order_by(Proxy.uptime).limit(1).to_url() # TODO add condition about yand block and geo
        return proxy

    @staticmethod
    def blacklist_proxy(proxy_string):
        proxy = Proxy.select().where()  # TODO finish: should pick all proxies with a banned address
        BannedByYandexProxy.create(_banned_at, _proxy_id=proxy.id, last_check=None)

    @staticmethod
    def free_from_blacklist(proxy):
        proxy.delete_instance()


if __name__ == "__main__":
    port_manager = DeleGatePortManager()
    port_manager.reserve_port(13)
    print(port_manager.used_ports)
    port_manager.release_port(13)
    print(port_manager.used_ports)
