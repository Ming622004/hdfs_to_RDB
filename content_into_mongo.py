import hdfs_oper
import json, datetime
from pymongo import MongoClient
import cb104_addr
# 將content寫進mongodb
paper_name = "storm"

conn_md = MongoClient(cb104_addr.local_mongodb_host)
mongodb_db_name = "news"
mongodb_collection = paper_name + "_news"
news_start_date = datetime.date(2019, 3, 15)
news_end_date = datetime.date(2019, 3, 23)
# ===============================

mdb = conn_md[mongodb_db_name]
mcollection = mdb[mongodb_collection]

path = "/" + paper_name + "_news_data/news/"
fileneme_suffix = "_" + paper_name + "_news.json"

filename_list = hdfs_oper.list(hdfs_oper.client,path)
# print(a)
# while os.path.exists(path + str(news_start_date) + fileneme_suffix):

# 如果之前沒創過 把link設定成唯一

m_data = mcollection.find_one()
if m_data is None:
    mcollection.create_index("news_link", unique=True)

while news_start_date <= news_end_date:
    filename = str(news_start_date) + fileneme_suffix
    if filename in filename_list:
        print("正在處理:", filename)
        with hdfs_oper.client.read(path+filename, encoding="utf-8") as reader:
            content = json.load(reader)
        # print(content)
        # 寫入 遇到錯誤會繼續 將link設成唯一可達到不重複寫入
        mcollection.insert_many(content["news"], ordered=False)
    news_start_date = news_start_date + datetime.timedelta(days=1)
