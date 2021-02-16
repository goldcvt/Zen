from ZenCrawlerSource.utils.models import Proxy
from psycopg2 import connect


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
    db_host = None
    db_uname = None
    db_pass = None
    proxy_db = None
    blacklist_db = None

    @staticmethod
    def connect_to_db(dbname):
        conn = connect(ProxyManager.db_host, ProxyManager.db_uname, ProxyManager.db_pass, dbname)
        return conn

    def __init__(self, proxy_db_conn=None, blacklist_db_conn=None):
        self.proxy_db_conn = proxy_db_conn
        self.blacklist_db_conn = blacklist_db_conn

    def get_proxy(self, proxy, dbname=proxy_db):
        if not self.proxy_db_conn:
            self.proxy_db_conn = self.connect_to_db(dbname)

    def blacklist_proxy(self, proxy, dbname=blacklist_db):
        if not self.blacklist_db_conn:
            self.blacklist_db_conn = self.connect_to_db(dbname)


if __name__ == "__main__":
    port_manager = DeleGatePortManager()
    port_manager.reserve_port(13)
    print(port_manager.used_ports)
    port_manager.release_port(13)
    print(port_manager.used_ports)
