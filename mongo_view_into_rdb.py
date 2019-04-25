import json, datetime, sys
from pymongo import MongoClient
from datetime import datetime
import MySQLdb
import jieba.analyse
import cb104_addr

paper_name = "udn"

mongodb_db_host = cb104_addr.local_mongodb_host
mongodb_db_name = "news"
mongodb_collection = paper_name + "_news"

mariadb_host = cb104_addr.local_mariadb_host
mariadb_user = cb104_addr.local_mariadb_user
mariadb_pw = cb104_addr.local_mariadb_pw
mariadb_db_name = "news_db"
mariadb_tablename = paper_name + "_news"

jieba.load_userdict("詞庫B2.txt")
# ===============================

conn_md = MongoClient(mongodb_db_host)
mdb = conn_md[mongodb_db_name]
mcollection = mdb[mongodb_collection]


# 更新資料庫views
def update_views(views_list, url):
    global cursor
    sql_value_def = ""
    for i in range(72):
        if views_list[i] is not None:
            sql_value_def = sql_value_def + "views_" + str(i + 1) + "=" + str(views_list[i]) + ","
    if sql_value_def == "":
        return
    sql_value_def = sql_value_def[:-1]
    sql_command_def = "update " + mariadb_tablename + " set " + sql_value_def + " where url = '" + url + "';"
    # print(sql_command_def)
    # print("views update OK")
    cursor.execute(sql_command_def)
    # conn.commit()


# keyword
def update_keyword(title_kw, news_kw, url):
    global cursor
    sql_value_def = ""
    for i3 in range(min(3, len(title_kw))):
        if title_kw[i3] is not None:
            sql_value_def = sql_value_def + "title_keyword_" + str(i3 + 1) + \
                        "='" + str(title_kw[i3]) + "',"
    for i3 in range(min(20, len(news_kw))):
        if news_kw[i3] is not None:
            sql_value_def = sql_value_def + "keyword_" + str(i3 + 1) + "='" + str(news_kw[i3]) + "',"
    if sql_value_def == "":
        return

    sql_value_def = sql_value_def[:-1]
    sql_command_def = "update " + mariadb_tablename + " set " + sql_value_def + \
                      " where url = '" + url + "';"
    # print(sql_command_def)
    # print("keyword update OK")
    cursor.execute(sql_command_def)
    # conn.commit()


count = 0
# 連線資料庫
conn = MySQLdb.connect(host=mariadb_host, user=mariadb_user, passwd=mariadb_pw, db=mariadb_db_name, charset="utf8")
cursor = conn.cursor()  # 傳回 Cursor 物件
for news_data in mcollection.find({}):
    try:
        count = count + 1
        if count % 100 == 0:
            print("處理檔案數:", count)
            conn.commit()
            cursor.close()
            conn.close()
            conn = MySQLdb.connect(host=mariadb_host, user=mariadb_user, passwd=mariadb_pw, db=mariadb_db_name,
                                   charset="utf8")
            cursor = conn.cursor()
        news_create_time = datetime.strptime(news_data["news_create_time"], '%Y-%m-%d %H:%M')
        news_time_temp = []
        news_view_temp = []
        # print(news_data)
        if "news_view" not in news_data:
            continue
        for news_view_single in news_data["news_view"]:
            # print(news_view_single)
            time1 = datetime.strptime(news_view_single["time"], '%Y-%m-%d %H:%M')
            if news_view_single["view"] is None:
                continue
            time_delta = (time1 - news_create_time).total_seconds()
            # 重複資料不要 超過四天資料不要
            if time_delta not in news_time_temp and time_delta < 350000:
                news_view_temp.append(int(news_view_single["view"]))
                news_time_temp.append(time_delta)

        # 資料時間差超過多少就不內插(先設定12小時)
        # 整理views資料至1小時一筆
        list_save = []
        for i in range(72):
            target_time = i * 3600 + 3600
            if target_time in news_time_temp:
                list_save.append(news_view_temp[news_time_temp.index(target_time)])
            else:
                # 這個找出最接近值的方法超爛 以後有機會要改
                time_upper_temp = []
                time_lower_temp = [0]
                for time_temp2 in news_time_temp:
                    if time_temp2 - target_time > 0:
                        time_upper_temp.append(time_temp2)
                    if target_time - time_temp2 > 0:
                        time_lower_temp.append(time_temp2)
                # 沒有比目標時間還要大的資料
                if len(time_upper_temp) == 0:
                    list_save.append(None)
                    continue
                upper_time = min(time_upper_temp)
                lower_time = max(time_lower_temp)
                # 判斷是否超過12小時
                if upper_time - lower_time > 43200:
                    list_save.append(None)
                    continue
                upper_view = news_view_temp[news_time_temp.index(upper_time)]
                if lower_time == 0:
                    lower_view = 0
                else:
                    lower_view = news_view_temp[news_time_temp.index(lower_time)]
                # 內差view寫進list
                list_save.append(
                    int((upper_view - lower_view) * (target_time - lower_time) / (upper_time - lower_time) + lower_view))

        # jieba
        news_content = news_data["news_content"]
        # 蘋果內文後面的廣告
        if "看了這則新聞的人" in news_content:
            pos = news_content.find("看了這則新聞的人")
            news_content = news_content[:pos]
            # print(news_content)
        news_content.replace("\r", "").replace("\n", "")
        news_key_word = jieba.analyse.extract_tags(news_content, topK=20, withWeight=False)
        title_key_word = jieba.analyse.extract_tags(news_data["news_title"], topK=3, withWeight=False)

        # 寫進MySQL
        if not len(list_save) == 72:
            print("views資料個數有問題")

        try:
            # 先判斷有沒有這個url
            sql_command = "select * from " + mariadb_tablename + " where url = '" + news_data["news_link"] + "';"
            # print(sql_command)
            cursor.execute(sql_command)
            finded_data = cursor.fetchall()
            # print(finded_data)
            data_count = len(finded_data)

            # 如果沒資料用insert 有資料用update
            if data_count == 0:
                # 沒有這個url 把keyword跟一些資料寫進去

                sql_column_name = ""
                sql_value = ""
                for i in range(len(title_key_word)):
                    sql_column_name = sql_column_name + "," + "title_keyword_" + str(i+1)
                    sql_value = sql_value + ",'" + title_key_word[i] + "'"

                for i in range(len(news_key_word)):
                    sql_column_name = sql_column_name + "," + "keyword_" + str(i+1)
                    sql_value = sql_value + ",'" + news_key_word[i] + "'"

                sql_command = \
                    "insert into " + mariadb_tablename + "(url,create_time,title,tag" + sql_column_name + \
                    ") values('" + news_data["news_link"] + "','" + news_data["news_create_time"] + "','" + \
                    news_data["news_title"] + "','" + news_data["news_tag"]\
                    + "'" + sql_value + ");"
                # print(sql_command)
                cursor.execute(sql_command)
                # conn.commit()

                # 內文寫完後更新views
                update_views(list_save, news_data["news_link"])
                # print("insert done")

            elif data_count == 1:
                # 有資料 先比對keyword
                error_check = False
                for i_tk in range(min(3, len(title_key_word))):
                    if not title_key_word[i_tk] == finded_data[0][i_tk + 3]:
                        error_check = True
                        break
                for i_kw in range(min(20, len(news_key_word))):
                    if not news_key_word[i_kw] == finded_data[0][i_kw+7]:
                        error_check = True
                        break
                # 有不一樣的就更新keyword
                if error_check:
                    update_keyword(title_key_word, news_key_word, news_data["news_link"])

                # 比對views
                for i2 in range(72):
                    if list_save[i2] is not None and not list_save[i2] == finded_data[0][i2 + 27]:
                        # 執行update
                        update_views(list_save, news_data["news_link"])
                        break
            else:
                print("資料庫有錯 有兩筆同樣的link")

            # print("done")

        except Exception as e:
            print("error 1")
            print(e)
    except Exception as e:
        print("error 2")
        print(e)
cursor.close()
conn.close()
