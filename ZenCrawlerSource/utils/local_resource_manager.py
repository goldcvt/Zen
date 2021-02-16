from ZenCrawlerSource.utils.models import Proxy, BannedByYandexProxy, raw_db
from psycopg2 import connect
from datetime import datetime


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

    def release_port(self, port):
        if self.used_ports is not None:
            self.used_ports.remove(port)
        else:
            pass


class ProxyManager:
    @staticmethod
    def get_proxy(proto):
        proxy = Proxy.select().where(Proxy.protocol == proto, Proxy.number_of_bad_checks == 0).order_by(
            Proxy.last_check_time.desc()).limit(1)
        if proxy:
            proxy = proxy.to_url(protocol=proto)
            return proxy

    @staticmethod
    def blacklist_proxy(proxy):
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
