#coding=utf-8
import json
import time

import requests

from node_start import ConsistentHashRing


# 获取用户的输入
author_name = input("Please enter the author name: ")
start_year = input("Please enter the start year: ")
end_year = input("Please enter the end year: ")

# 你的服务的地址和端口
base_url1 = "http://127.0.0.1:8081"
base_url2 = "http://127.0.0.1:8082"
base_url3 = "http://127.0.0.1:8083"



url1 = f"{base_url1}/total/{author_name}/{start_year}/{end_year}"
url2 = f"{base_url2}/total/{author_name}/{start_year}/{end_year}"
url3 = f"{base_url3}/total/{author_name}/{start_year}/{end_year}"
url4 = f"{base_url1}/test/{author_name}/{start_year}/{end_year}"
url5 = f"{base_url2}/test/{author_name}/{start_year}/{end_year}"
url6 = f"{base_url3}/test/{author_name}/{start_year}/{end_year}"


chr_instance = ConsistentHashRing()


def query_indexing():
    total = 0
    total1 = 0
    total2 = 0
    total3 = 0

    start_time = time.time()
    try:
        response1 = requests.get(url1)
        print(response1.text)
        total1 = int(response1.text.split(",")[0].split(":")[1].strip())
        total += total1
    except Exception as e:
        print(e)
        detail_url = f"{base_url2}/details"
        response = requests.get(detail_url)
        data_dict = json.loads(response.text)
        chr_instance.set(data_dict["node"], data_dict["nodes"], data_dict["ring"], data_dict["sorted_keys"])

        ring_data = data_dict.get('ring', {})

        # 获取所有的key为node1的键值对
        keys = {key: value for key, value in ring_data.items() if value == "node1"}
        # 获取备份节点
        new_keys = {}
        # 遍历原始的字典
        for key in keys:
            new_keys[key] = chr_instance.get_next_virtual_node(int(key))
        print("new_keys:")
        print(new_keys)

        # 发送请求进行查询
        for new_key, value in new_keys.items():
            if value == "node2":
                url = f"{base_url2}/total/{author_name}/{start_year}/{end_year}/{new_key}"
            else:
                url = f"{base_url3}/total/{author_name}/{start_year}/{end_year}/{new_key}"
            response = requests.get(url)
            print(response.text)
            total1 += int(response.text.split(",")[0].split(":")[1].strip())

        print("total1:" + str(total1))
        total += total1
        # data_dict = json.loads(response.ring)

    try:
        response2 = requests.get(url2)
        print(response2.text)
        total2 = int(response2.text.split(",")[0].split(":")[1].strip())
        total += total2
    except Exception as e:
        print(e)
        detail_url = f"{base_url3}/details"
        response = requests.get(detail_url)
        data_dict = json.loads(response.text)

        # 获取最新的哈希环，并对本地的数据结构进行更新
        chr_instance.set(data_dict["node"], data_dict["nodes"], data_dict["ring"], data_dict["sorted_keys"])

        ring_data = data_dict.get('ring', {})

        # 获取所有的key为node1的键值对
        keys = {key: value for key, value in ring_data.items() if value == "node2"}
        # 获取备份节点
        new_keys = {}
        # 遍历原始的字典
        for key in keys:
            new_keys[key] = chr_instance.get_next_virtual_node(int(key))
        print("new_keys:")
        print(new_keys)

        # 发送请求进行查询
        for new_key, value in new_keys.items():
            if value == "node3":
                url = f"{base_url3}/total/{author_name}/{start_year}/{end_year}/{new_key}"
            else:
                url = f"{base_url1}/total/{author_name}/{start_year}/{end_year}/{new_key}"
            response = requests.get(url)
            # print(response.text)
            total2 += int(response.text.split(",")[0].split(":")[1].strip())

        print("total2:" + str(total1))
        total += total2

    try:
        response3 = requests.get(url3)
        print(response3.text)
        total3 = int(response3.text.split(",")[0].split(":")[1].strip())
        total += total3
    except Exception as e:
        print(e)
        detail_url = f"{base_url1}/details"
        response = requests.get(detail_url)
        data_dict = json.loads(response.text)

        # 获取最新的哈希环，并对本地的数据结构进行更新
        chr_instance.set(data_dict["node"], data_dict["nodes"], data_dict["ring"], data_dict["sorted_keys"])
        ring_data = data_dict.get('ring', {})

        # 获取所有的key为node1的键值对
        keys = {key: value for key, value in ring_data.items() if value == "node3"}
        # 获取备份节点
        new_keys = {}
        # 遍历原始的字典
        for key in keys:
            new_keys[key] = chr_instance.get_next_virtual_node(int(key))
        print("new_keys:")
        print(new_keys)

        # 发送请求进行查询
        for new_key, value in new_keys.items():
            if value == "node2":
                url = f"{base_url2}/total/{author_name}/{start_year}/{end_year}/{new_key}"
            else:
                url = f"{base_url1}/total/{author_name}/{start_year}/{end_year}/{new_key}"
            response = requests.get(url)
            # print(response.text)
            total3 += int(response.text.split(",")[0].split(":")[1].strip())

        print("Query without indexing:")
        print("total3:" + str(total1))
        total += total3

    end_time = time.time()

    print("total number:" + str(total))
    print("time:" + str(end_time - start_time))


def query_without_indexing():
    total = 0
    total1 = 0
    total2 = 0
    total3 = 0

    start_time = time.time()
    response1 = requests.get(url4)
    print(response1.text)
    total1 = int(response1.text.split(",")[0].split(":")[1].strip())
    total += total1

    response2 = requests.get(url5)
    print(response2.text)
    total2 = int(response2.text.split(",")[0].split(":")[1].strip())
    total += total2

    response3 = requests.get(url6)
    print(response3.text)
    total3 = int(response3.text.split(",")[0].split(":")[1].strip())
    total += total3

    end_time = time.time()

    print("Query without indexing:")
    print("total number:" + str(total))
    print("time:" + str(end_time - start_time))


if __name__ == "__main__":
    query_indexing()
    query_without_indexing()