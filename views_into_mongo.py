import hdfs_oper
import json, datetime
from datetime import datetime, date
from pymongo import MongoClient
import cb104_addr
# 將content寫進mongodb

# 新聞名字 起始日期 結束日期
paper_name = "apple"
conn = MongoClient(cb104_addr.ray_GCP)
news_start_date = date(2019, 1, 30)
news_end_date = date(2019, 3, 2)

mdb = conn.news
paper_name + "_news"
mcollection = mdb[paper_name + "_news"]

path = "/" + paper_name + "_news_data/views/"
filename_suffix = "_" + paper_name + "_news_view.json"

filename_list = hdfs_oper.list(hdfs_oper.client,path)

# 檔案日期判斷
while news_start_date <= news_end_date:
    filename = str(news_start_date) + filename_suffix
    if filename in filename_list:
        print("正在處理:", filename)
        with hdfs_oper.client.read(path + filename, encoding="utf-8") as reader:
            content = json.load(reader)
        for view_single in content["views"]:
            # 如果資料庫存存在此link之json檔案
            for m_data in mcollection.find({"news_link": view_single["news_link"]}):
                # 新聞創立時間
                news_create_time = datetime.strptime(m_data["news_create_time"], '%Y-%m-%d %H:%M')
                time1 = datetime.strptime(view_single["time"], '%Y-%m-%d %H:%M')
                time_delta = (time1 - news_create_time).total_seconds()
                # 超過五天資料不要
                count = 0 # 測試用count
                if time_delta <= 432000 and {"view": view_single["view"], "time": view_single["time"]} not in m_data["news_view"]:
                    count = count+1
                    # 測試用count 確認有寫進DB
                    if count == 50:
                        print("已輸入50筆資料")
                        count = 0
                    view_insert = {"news_view": {"view": view_single["view"], "time": view_single["time"]}}
                    mcollection.update({"news_link": view_single["news_link"]}, {"$push": view_insert})

    news_start_date = news_start_date + datetime.timedelta(days=1)

# /apple_news_data/views/2019-02-02_apple_news_view.json
# /apple_news_data/news/2019-01-22_apple_news.json
