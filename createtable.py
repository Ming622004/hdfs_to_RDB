# install mysqlclient
import MySQLdb
import cb104_addr

# apple storm CT udn
papername = "CT"

mariadb_db_name = "news_db"
mariadb_db_table = papername + "_news"
mariadb_host = cb104_addr.ray_MySQL_IP
mariadb_user = cb104_addr.ray_MySQL_username
mariadb_pw = cb104_addr.ray_MySQL_password

conn_rdb = MySQLdb.connect(host=mariadb_host, user=mariadb_user,
                           passwd=mariadb_pw, db=mariadb_db_name, charset="utf8")
cursor = conn_rdb.cursor()           #傳回 Cursor 物件

SQL_CT = "CREATE TABLE IF NOT EXISTS " + mariadb_db_table + "(url varchar(255) PRIMARY KEY,\
         create_time DATETIME, \
         title varchar(255), \
         title_keyword_1 varchar(100), \
         title_keyword_2 varchar(100), \
         title_keyword_3 varchar(100), \
         tag varchar(100), \
         keyword_1 varchar(100), \
         keyword_2 varchar(100), \
         keyword_3 varchar(100), \
         keyword_4 varchar(100), \
         keyword_5 varchar(100), \
         keyword_6 varchar(100), \
         keyword_7 varchar(100), \
         keyword_8 varchar(100), \
         keyword_9 varchar(100), \
         keyword_10 varchar(100), \
         keyword_11 varchar(100), \
         keyword_12 varchar(100), \
         keyword_13 varchar(100), \
         keyword_14 varchar(100), \
         keyword_15 varchar(100), \
         keyword_16 varchar(100), \
         keyword_17 varchar(100), \
         keyword_18 varchar(100), \
         keyword_19 varchar(100), \
         keyword_20 varchar(100), \
         views_1 int, \
         views_2 int, \
         views_3 int, \
         views_4 int, \
         views_5 int, \
         views_6 int, \
         views_7 int, \
         views_8 int, \
         views_9 int, \
         views_10 int, \
         views_11 int, \
         views_12 int, \
         views_13 int, \
         views_14 int, \
         views_15 int, \
         views_16 int, \
         views_17 int, \
         views_18 int, \
         views_19 int, \
         views_20 int, \
         views_21 int, \
         views_22 int, \
         views_23 int, \
         views_24 int, \
         views_25 int, \
         views_26 int, \
         views_27 int, \
         views_28 int, \
         views_29 int, \
         views_30 int, \
         views_31 int, \
         views_32 int, \
         views_33 int, \
         views_34 int, \
         views_35 int, \
         views_36 int, \
         views_37 int, \
         views_38 int, \
         views_39 int, \
         views_40 int, \
         views_41 int, \
         views_42 int, \
         views_43 int, \
         views_44 int, \
         views_45 int, \
         views_46 int, \
         views_47 int, \
         views_48 int, \
         views_49 int, \
         views_50 int, \
         views_51 int, \
         views_52 int, \
         views_53 int, \
         views_54 int, \
         views_55 int, \
         views_56 int, \
         views_57 int, \
         views_58 int, \
         views_59 int, \
         views_60 int, \
         views_61 int, \
         views_62 int, \
         views_63 int, \
         views_64 int, \
         views_65 int, \
         views_66 int, \
         views_67 int, \
         views_68 int, \
         views_69 int, \
         views_70 int, \
         views_71 int, \
         views_72 int)"

cursor.execute(SQL_CT)

conn_rdb.commit()

cursor.close()
conn_rdb.close()