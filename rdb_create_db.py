# install mysqlclient
import MySQLdb
import cb104_addr

conn = MySQLdb.connect(host=cb104_addr.ray_MySQL_IP, user=cb104_addr.ray_MySQL_username, passwd=cb104_addr.ray_MySQL_password)

cursor=conn.cursor()     #傳回 Cursor 物件
cursor.execute("SELECT VERSION()")     #查詢資料庫版本

print("Database version : %s " % cursor.fetchone())

SQL = "CREATE DATABASE IF NOT EXISTS news DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_unicode_ci"
cursor.execute(SQL)

conn.commit()

cursor.close()
conn.close()
