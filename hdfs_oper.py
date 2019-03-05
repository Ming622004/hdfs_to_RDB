# install hdfs
from hdfs import InsecureClient, Config
from hdfs.client import Client
import json
from pymongo import MongoClient
# client = InsecureClient('http://10.120.14.110:50070', user='root')

# client = Config().get_client('dev')

# with client.read('/') as reader:
#   features = reader.read()
#   print(features)


def read_hdfs_file(client , filename):
    # with client.read('samples.csv', encoding='utf-8', delimiter='\n') as reader:
    #  for line in reader:
    # pass
    lines = []
    with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
        for line in reader:
            # pass
            # print line.strip()
            lines.append(line.strip())
    return lines


# 创建目录
def mkdirs(client, hdfs_path):
    client.makedirs(hdfs_path)


# 删除hdfs文件
def delete_hdfs_file(client, hdfs_path):
    client.delete(hdfs_path)


# 上传文件到hdfs
def put_to_hdfs(client, local_path, hdfs_path):
    client.upload(hdfs_path, local_path, cleanup=True)


# 从hdfs获取文件到本地
def get_from_hdfs(client, hdfs_path, local_path):
    client.download(hdfs_path, local_path, overwrite=False)


# 追加数据到hdfs文件
def append_to_hdfs(client, hdfs_path, data):
    client.write(hdfs_path, data, overwrite=False, append=True, encoding='utf-8')


# 覆盖数据写到hdfs文件
def write_to_hdfs(client, hdfs_path, data):
    client.write(hdfs_path, data, overwrite=True, append=False, encoding='utf-8')


# 移动或者修改文件
def move_or_rename(client, hdfs_src_path, hdfs_dst_path):
    client.rename(hdfs_src_path, hdfs_dst_path)


# 返回目录下的文件
def list(client, hdfs_path):
    return client.list(hdfs_path, status=False)

# client = Client(url, root=None, proxy=None, timeout=None, session=None)
# client = Client("http://hadoop:50070")


client = Client(url="http://master:50070", root="/", timeout=10000, session=False)
# client = InsecureClient("http://120.78.186.82:50070", user='ann');

# rootfile = list(client, "/")
#print(rootfile)

# filename = "/CT_news_data/news/2019-02-01_CT_news.json"
# get_from_hdfs(client, filename, "./")

# client.read(filename, encoding="utf-8")
# with client.read(filename, encoding="utf-8") as reader:
#     # print(reader)
#     content = json.load(reader)
# file = list(client, filename)

# print(content)
# print(len(content["news"]))
#
# conn = MongoClient("127.0.0.1")
# mdb = conn.test
# collection = mdb.xyz
#
# for content_single in content["news"]:
#     print(content_single)
#     collection.insert_one(content_single)