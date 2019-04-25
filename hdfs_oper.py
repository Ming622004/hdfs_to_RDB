from hdfs.client import Client


# 因為設定上的原因
# 要先到 C:\Windows\System32\drivers\etc 設定
# 10.120.14.110 master 等等
def read_hdfs_file(client, filename):
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


client = Client(url="http://master:50070", root="/", timeout=10000, session=False)
