import hdfs_oper
import json
from datetime import date
import datetime
from pymongo import MongoClient
import cb104_addr
# 將content寫進mongodb

# 新聞名字 起始日期 結束日期
paper_name = "apple"

conn_md = MongoClient(cb104_addr.local_mongodb_host)
mongodb_db_name = "news"
mongodb_collection = paper_name + "_news"
news_start_date = date(2019, 3, 14)
news_end_date = date(2019, 3, 23)
# ===============================

mdb = conn_md[mongodb_db_name]
mcollection = mdb[mongodb_collection]

path = "/" + paper_name + "_news_data/views/"
filename_suffix = "_" + paper_name + "_news_view.json"

filename_list = hdfs_oper.list(hdfs_oper.client,path)

# 檔案日期判斷
count = 0
while news_start_date <= news_end_date:
    filename = str(news_start_date) + filename_suffix
    if filename in filename_list:
        print("正在處理:", filename)
        with hdfs_oper.client.read(path + filename, encoding="utf-8") as reader:
            # content_check = reader.read()
            # print(content_check[19709800:19710000])
            content = json.load(reader)
            # print(content)
        for view_single in content["views"]:
            if "https://" not in view_single["news_link"] and paper_name == "CT":
                view_single["news_link"] = "https://" + view_single["news_link"]
            # 如果資料庫存存在此link之json檔案
            for m_data in mcollection.find({"news_link": view_single["news_link"]}):
                # 新聞創立時間
                # print(m_data["news_create_time"])
                news_create_time = datetime.datetime.strptime(m_data["news_create_time"], '%Y-%m-%d %H:%M')
                time1 = datetime.datetime.strptime(view_single["time"], '%Y-%m-%d %H:%M')
                time_delta = (time1 - news_create_time).total_seconds()
                if "news_view" not in m_data:
                    m_data["news_view"] = []
                # 超過五天資料不要 重複不要
                try:
                    if time_delta <= 432000 and {"view": view_single["view"], "time": view_single["time"]} not in m_data["news_view"]:
                        view_insert = {"news_view": {"view": view_single["view"], "time": view_single["time"]}}
                        mcollection.update({"news_link": view_single["news_link"]}, {"$push": view_insert})
                        count = count + 1
                        if count % 50000 == 0:
                            print("已經處理:", count)
                except Exception as e:
                    print(e)

    news_start_date = news_start_date + datetime.timedelta(days=1)

# /apple_news_data/views/2019-02-02_apple_news_view.json
# /apple_news_data/news/2019-01-22_apple_news.json
