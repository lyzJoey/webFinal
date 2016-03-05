import MySQLdb
from yahoo_finance import Share
from datetime import datetime,date,timedelta
import time;
import csv;
import msvcrt;
def sqlImplement(sql, cursor, db):
	try:
		cursor.execute(sql)
		db.commit()
	except:
		db.rollback()

#def generateCSV(name):
db = MySQLdb.connect("localhost","root","921020");
cursor = db.cursor();
sql = "create database if not exists Stock"
cursor.execute(sql);
cursor.execute("use Stock");

sql = "create table if not exists Company\
		(name varchar(10) not null,\
		primary key(name))"
cursor.execute(sql);
sql = "create table if not exists oneYearStock\
		(name varchar(10) not null, \
		time Date not null,\
		open double not null,\
		close double not null, \
		high double not null,\
		low double not null,\
		volume double not null,\
		primary key(name,time),\
		foreign key(name) references Company(name))"
cursor.execute(sql);
sql = "create table if not exists oneDayStock\
		(name varchar(10) not null,\
		time DateTime not null,\
		price double not null,\
		volume double not null,\
		primary key(name,time),\
		foreign key(name) references Company(name))"
cursor.execute(sql);
print ("finish create table");
#company_name = ["YHOO","GOOG","FB","AMZN","MSFT"];
company_name = ["YHOO"];
for name in company_name:
	sql = "select * from Company\
		where name = '%s'" % (name);
	result = cursor.execute(sql);
	#print result
	if(result == 0):
		sql = "insert into Company(name) values('%s')" %(name);
		sqlImplement(sql, cursor, db);
print ("finish insert company table");
today = date.today() - timedelta(days = 1);
now  = datetime.now() - timedelta(minutes = 1);
oneYearBefore = today - timedelta(days = 365);

for name in company_name:
	stock = Share(name);
	result = stock.get_historical(str(oneYearBefore),str(today));
	for ret in result:
		name = ret['Symbol']
		day = ret['Date']
		sql = "select * from oneYearStock\
				where name = '%s' and time = '%s'" %(name,day)
		reaction = cursor.execute(sql)
		if reaction == 0:
			Open = ret['Open']
			Close = ret['Close']
			Volume = ret["Volume"]
			High = ret["High"]
			Low = ret['Low']
			sql = "insert into oneYearStock(name,time,open,close,high,low,volume) values('%s','%s','%s','%s','%s','%s','%s')" %(name,day,Open,Close,High,Low,Volume)
			sqlImplement(sql, cursor, db);
#above is initialize
print ("finish initialize  oneYearStock table");
openTime = datetime.now().replace(hour = 9, minute = 30, second = 0);
closeTime = datetime.now().replace(hour = 16, minute = 0, second = 0);
print ("program start");
flag = True;
stop = False;
while True:
	if openTime < datetime.now() <= closeTime and datetime.now().minute != now.minute:
		now = datetime.now()
		for name in company_name:
			stock = Share(name)
			price = stock.get_price()
			volume = stock.get_volume()
			sql = "insert into oneDayStock(name,time,price,volume) values('%s','%s','%s','%s')" % (str(name),str(now),str(price),str(volume))
			sqlImplement(sql, cursor, db);
		
		print ("click y to ouput tillnow csv in 10 sec, if not continue running program");
		start_time = time.time();
		while 1:
			while msvcrt.kbhit():
				msvcrt.getch();	
			if msvcrt.kbhit():
				key = ord(msvcrt.getche());
				if key in map(ord, 'yY'):
					flag = False;
					print ('');
					print ("start generate onedaystock file");
					for name in company_name:
						tmp = ('oneDayStock_%s' +'.csv') %(name);
						oneDayStock = file(tmp, 'wb');
						writer = csv.writer(oneDayStock, dialect = 'excel');
						writer.writerow(['name', 'time', 'price', 'volume']);
						sql = "select * from oneDayStock where name = '%s'" %(str(name));
						cursor.execute(sql);
						res = cursor.fetchall();
						for row in res:
							writer.writerow(row);
						oneDayStock.close();
				if time.time() - start_time < 10:
					time.sleep(10-(time.time() - start_time));
				break;
			elif time.time() - start_time > 10:
				break;
		print ("go on programming");
		
	if datetime.now().hour > 16:
		sql = "truncate table oneDayStock";
		sqlImplement(sql, cursor, db);
		for name in company_name:
			stock = Share(name);
			Open = stock.get_open();
			Volume = stock.get_volume();
			High = stock.get_days_high();
			Low = stock.get_days_low();
			sql = "delete from oneYearStock where time = '%s'" %oneYearBefore;
			sqlImplement(sql, cursor, db);
			today = date.today();
			while True:
				if today != date.today():
					Close = stock.get_prev_close();
					sql = "insert into oneYearStock(name,time,open,close,high,low,volume) values('%s','%s','%s','%s','%s','%s','%s')" %(str(name),str(today),str(Open),str(Close),str(High),str(Low),str(Volume));
					sqlImplement(sql, cursor, db);
				else:
					time.sleep(3600);
			oneYearBefore = today - timedelta(days = 365);
	print ("give u 10 sec to click y to jummp out of the program");
	start_time = time.time();
	while 1:
		while msvcrt.kbhit():
			msvcrt.getch();
		if msvcrt.kbhit():
			key = ord(msvcrt.getche());
			if key in map(ord, 'yY'):
				print ('');
				print ('jump out of program');
				stop = True;
				break;
		elif time.time() - start_time >10:
			break;
	if stop:
		break;
	print ("go on");
	time.sleep(40);

#sql = "select * from oneYearStock into outfile 'E:\\oneYearStock.txt\
#		fields terminated by ',' enclosed by '""'\
#		lines terminated by '\r\n'";
#sqlImplement(sql, cursor);
#sql = "select * from oneDyaStock into outfile 'E:\\oneDyaStock.txt\
#		fields terminated by ',' enclosed by '""'\
#		lines terminated by '\r\n'";
#sqlImplement(sql, cursor);

if flag:
	for name in company_name:
		tmp = ('oneDayStock_%s' +'.csv') %(name);
		oneDayStock = file(tmp, 'wb');
		writer = csv.writer(oneDayStock, dialect = 'excel');
		writer.writerow(['name', 'time', 'price', 'volume']);
		sql = "select * from oneDayStock where name = '%s'" %(str(name));
		cursor.execute(sql);
		res = cursor.fetchall();
		for row in res:
			writer.writerow(row);
		oneDayStock.close();
for name in company_name:
	tmp = ('oneYearStock_%s' +'.csv') %(name);
	oneYearStock = file(tmp, 'wb');
	writer = csv.writer(oneYearStock, dialect = 'excel');
	writer.writerow(['name', 'time', 'open_price', 'close_price', 'dailyHigh', 'dailyLow', 'volume']);
	sql = "select * from oneYearStock where name = '%s'" %(name);
	cursor.execute(sql);
	res = cursor.fetchall();
	for row in res:
		writer.writerow(row);
	oneYearStock.close();
print ('finish');
db.close();