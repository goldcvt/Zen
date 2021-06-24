import psycopg2

def del_from_db(connection, tablename, **kwargs):
	cursor = connection.cursor()
	conditions = "".join(" {}={}".format(k.lower(), v) for k,v in iter(kwargs.items()))
	request = "DELETE FROM {} WHERE ({});".format(tablename, conditions) #возможно, нужны плейсхолдеры %s вместо {}
	# print(cursor.mogrify(request))
	# ЧИСТО ДЛЯ ТЕСТОВ
	# if ask() == True:
	cursor.execute(request)
	connection.commit()
	cursor.close()

def read_from_db(connection, tablename, *columns, **filters): #когда в вызове пихаешь аргументы в виде списка/словаря перед ними звездочки надо
#но нахуй писать лишние словари?)
	curs = connection.cursor()
	filts = "".join(" {} {}".format(" ".join(k.split(sep="_")).upper(), str(v)) for k,v in iter(filters.items()))
	cols = ",".join("{}".format(str(y)) for y in columns)
	request = "SELECT ({}) FROM {}{};".format(cols, tablename, filts) #возможно, нужны плейсхолдеры %s вместо {}
	# print(curs.mogrify(request))
	# ЧИСТО ДЛЯ ТЕСТОВ, УДАЛИ ПОТОМ
	# if ask() == True:
	curs.execute(request)
	connection.commit()
	res = curs.fetchall()
	return res
	curs.close()


def connect_to_db(basename, username, pswd, dbip):
	connection = psycopg2.connect(dbname=basename, user=username, password=pswd, host=dbip)
	return connection
	#че, после закрытия соединения каждый божий раз придётся к базе подключаться?))
	#да, поэтому не закрываем до победного конца))

def write_to_db(connection, tablename, **kwargs):
	cursor = connection.cursor()
	request = "INSERT INTO {} ({}) VALUES ({});".format(tablename, ",".join(x for x in kwargs.keys()), 
		",".join(str(x) for x in kwargs.values()))
	# print(cursor.mogrify(request))
	# ЧИСТО ДЛЯ ТЕСТОВ, УДАЛИ ПОТОМ
	# if ask() == True:
	cursor.execute(request)
	connection.commit()
	cursor.close()
#-------------------------ЛОГИЧЕСКАЯ ДОБАВЧОКА---------------------------------------------
def ask():
	answer = input("\nYou can see the request above. Do you wish to proceed? Y/n")
	if str(answer).lower() == 'y':
		return True
		print('True')
	else:
		print("You don't seem to like the idea, aborting")
		return False
