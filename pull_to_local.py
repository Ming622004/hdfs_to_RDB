import hdfs_oper
import json, datetime, os, sys
from pymongo import MongoClient
import cb104_addr
# 將資料拉到本地端
paper_name = "CT"
target = "news"

conn_md = MongoClient(cb104_addr.local_mongodb_host)
mongodb_db_name = "news"
mongodb_collection = paper_name + "_news"
news_start_date = datetime.date(2019, 3, 15)
news_end_date = datetime.date(2019, 3, 23)
# ===============================


mdb = conn_md[mongodb_db_name]
mcollection = mdb[mongodb_collection]

hdfsPath = "/" + paper_name + "_news_data/" + target + "/"
fileneme_suffix = "_" + paper_name + "_news.json"

filename_list = hdfs_oper.list(hdfs_oper.client, hdfsPath)
print(filename_list)

dataRootPath = "E:\\newsFileHdfs"

dataPaperPath = dataRootPath + "\\" + paper_name
# if not os.path.exists(dataPaperPath):
#     # print("data 資料夾不存在, 現在幫你創唷")
#     os.mkdir(dataPaperPath)

dataViewsPath = dataPaperPath + "\\" + target
# if not os.path.exists(dataViewsPath):
#     # print("data 資料夾不存在, 現在幫你創唷")
#     os.mkdir(dataViewsPath)

hdfs_oper.get_from_hdfs(hdfs_oper.client, "/" + paper_name + "_news_data", dataRootPath)

# for filename in filename_list:
#     with hdfs_oper.client.read(hdfsPath + filename, encoding="utf-8") as fileTemp:
#         print(fileTemp)
#         sys.exit()
# newsFileHdfs
