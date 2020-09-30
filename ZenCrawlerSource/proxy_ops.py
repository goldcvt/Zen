from crawler_tools import db_ops
import datetime


class Proxy:
	def __init__(self, proxy_id, proxy_type, auth_data, domain, port, response_time):
		self.id = proxy_id
		self.type = proxy_type
		self.auth_data = auth_data
		self.domain = domain
		self.port = port
		self.response_time = response_time

	def define_type(self):
		if self.type == "0":
			self.type = "http"
		elif self.type == "1":
			self.type = "socks4"
		elif self.type == "2":
			self.type = "socks5"
		else:
			self.type = "nonsense"

	def get_address(self):
		self.define_type()
		if self.auth_data == '""':
			return f"{self.type}://{self.domain}:{self.port}"
		else:
			print("Внезапно! Требуется авторизация. Такого мы не ожидали")
			return ""

	def blacklist(self, connection):
		tmp = db_ops.read_from_db(connection, "banned_by_yandex", "times_blacklisted", where = "proxy_db_id={}".format(self.id))
		timez = 1
		if tmp: # TODO update lib
			timez += int(tmp[0][0])
			db_ops.write_to_db(connection, "banned_by_yandex", proxy_db_id = self.id, times_blacklisted = timez)
		else:
			db_ops.write_to_db(connection, "banned_by_yandex", proxy_db_id = self.id, times_blacklisted = timez, blacklisted_at ="TIMESTAMP '{}'".format(datetime.datetime.now())) 
			# seconds + 3 dec milisecs

	def is_blacklisted(self, connection):
		# self.define_type() - ENORMOUS BUG, LUL
		is_found = db_ops.read_from_db(connection, "banned_by_yandex", "proxy_db_id", where = "proxy_db_id={}".format(self.id))
		if not is_found: # TODO update lib....
			return False
		else:
			return True

	def resurrect(self, connection):
		then = db_ops.read_from_db(connection, "banned_by_yandex", "blacklisted_at", where = f"proxy_db_id={self.id}")[0][0]
		# then = datetime.datetime.strptime(tmp, "%Y/%m/%d %H/%M/%S")
		if (datetime.datetime.now() - then).days >= 7:
			db_ops.del_from_db(connection, "banned_by_yandex", proxy_db_id = self.id)
			try:
				return True
			except Exception as e:
				print("Fucked up while deleting")
				raise e
		else:
			return False

	# like an actual function
	# @staticmethod
	# def get_proxy(connection):
	# 	conn = connection
	# 	list_proxy = db_ops.read_from_db(conn, "proxies", 'id', 'raw_protocol', 'auth_data',
	# 		'domain', 'port', 'response_time', order_by = "response_time", limit = 1)[0][0].translate(str.maketrans("a","a", "()")).split(",")
	# 	# print('This IS LIST PROXY' + str(list_proxy))
	# 	proxy = Proxy(list_proxy[0], list_proxy[1], list_proxy[2], list_proxy[3], list_proxy[4], list_proxy[5])
	# 	return proxy
	# 	#0 - http
	# 	#1 - socks4
	# 	#2 - socks5

	@staticmethod
	def get_random_proxy(connection, raw_protocol, bad_checks):
		conn = connection
		curr_proxy = db_ops.read_from_db(conn, "proxies", 'id', 'raw_protocol', 'auth_data',
										 'domain', 'port', 'response_time',
										 where="random() < 0.01 AND number_of_bad_checks = {} AND raw_protocol = {}"
										 .format(bad_checks, raw_protocol),
										 limit=1)[0][0].translate(
				str.maketrans("a", "a", "()")).split(",")
		if curr_proxy:
			proxy = Proxy(curr_proxy[0], curr_proxy[1], curr_proxy[2], curr_proxy[3], curr_proxy[4], curr_proxy[5])
			while proxy.is_blacklisted(conn):
				curr_proxy = db_ops.read_from_db(conn, "proxies", 'id', 'raw_protocol', 'auth_data',
												 'domain', 'port', 'response_time',
												 where="random() < 0.01 AND number_of_bad_checks = {} AND raw_protocol = {}"
												 .format(bad_checks, raw_protocol),
												 limit=1)[0][0].translate(
						str.maketrans("a", "a", "()")).split(",")
		return proxy

	@staticmethod
	def get_type_proxy(connection, raw_protocol, bad_checks):
		conn = connection
		proxy = None
		list_proxy = db_ops.read_from_db(conn, "proxies", "id", "raw_protocol", "auth_data",
										  "domain", "port", "response_time",
										  where="number_of_bad_checks={} AND raw_protocol={}".format(bad_checks,
																									 raw_protocol),
										  order_by="response_time", limit=1)[0][0].translate(
				str.maketrans("a", "a", "()")).split(",")
		# TODO а что если раньше были плохие проверки, а щас норм?)
		if list_proxy:
			proxy = Proxy(list_proxy[0], list_proxy[1], list_proxy[2], list_proxy[3], list_proxy[4],
						  list_proxy[5])
			while proxy.is_blacklisted(conn):
				proxy = Proxy.get_random_proxy(conn, raw_protocol, bad_checks)
				# TODO переписать на более интеллектуальный запрос, чтобы прочекивало в БД с условием нахождения в
				# другой таблице
		return proxy

	@staticmethod
	def get_from_string(connection, address):
		# parsing whole address into domain and port
		domain = "".join(address.split(":")[1]).replace("//", "")
		port = "".join(address.split(":")[2])
		list_proxy = db_ops.read_from_db(connection, "proxies", "id", "raw_protocol", "auth_data",
										  "domain", "port", "response_time",
										  where = "domain={} AND port = {} AND raw_protocol=0".format(domain, port),
										  order_by = "response_time", limit=1)[0][0].translate(
				str.maketrans("a", "a", "()")).split(",")
		proxy = Proxy(list_proxy[0], list_proxy[1], list_proxy[2], list_proxy[3], list_proxy[4], list_proxy[5])
		return proxy
