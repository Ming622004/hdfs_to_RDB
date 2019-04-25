# install mysqlclient
import MySQLdb
import cb104_addr

mariadb_db_name = "news_db"
mariadb_host = cb104_addr.ray_MySQL_IP
mariadb_user = cb104_addr.ray_MySQL_username
mariadb_pw = cb104_addr.ray_MySQL_password

conn = MySQLdb.connect(host=mariadb_host, user=mariadb_user, passwd=mariadb_pw)

cursor=conn.cursor()     #傳回 Cursor 物件
cursor.execute("SELECT VERSION()")     #查詢資料庫版本

print("Database version : %s " % cursor.fetchone())

SQL = "CREATE DATABASE IF NOT EXISTS " + mariadb_db_name + " DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_unicode_ci"
cursor.execute(SQL)

conn.commit()

cursor.close()
conn.close()
