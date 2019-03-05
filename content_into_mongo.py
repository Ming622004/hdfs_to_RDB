import hdfs_oper
import json, datetime
from pymongo import MongoClient
# 將content寫進mongodb
paper_name = "apple"

conn = MongoClient("127.0.0.1")
mdb = conn.news
# paper_name + "_news"
mcollection = mdb[paper_name + "_news"]

news_start_date = datetime.date(2019, 1, 30)
news_end_date = datetime.date(2019, 3, 3)
path = "/" + paper_name + "_news_data/news/"
fileneme_suffix = "_" + paper_name + "_news.json"

filename_list = hdfs_oper.list(hdfs_oper.client,path)
# print(a)
# while os.path.exists(path + str(news_start_date) + fileneme_suffix):

while news_start_date <= news_end_date:
    filename = str(news_start_date) + fileneme_suffix
    if filename in filename_list:
        print("正在處理:", filename)
        with hdfs_oper.client.read(path+filename, encoding="utf-8") as reader:
            content = json.load(reader)
        # 寫入 遇到錯誤會繼續 將link設成唯一可達到不重複寫入
        mcollection.insert_many(content["news"], ordered=False)
    news_start_date = news_start_date + datetime.timedelta(days=1)
