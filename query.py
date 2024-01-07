#coding=utf-8
import json

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

total = 0
total1 = 0
total2 = 0
total3 = 0

url1 = f"{base_url1}/total/{author_name}/{start_year}/{end_year}"
url2 = f"{base_url2}/total/{author_name}/{start_year}/{end_year}"
url3 = f"{base_url3}/total/{author_name}/{start_year}/{end_year}"

try:
    response1 = requests.get(url1)
    print(response1.text)
    total1 = int(response1.text.split(",")[0].split(":")[1].strip())
    total += total1
except Exception as e:
    print(e)
    config_url = f"{base_url2}/config"
    response = requests.get(config_url)
    data_dict = json.loads(response.text)
    ring_data = data_dict.get('ring', {})

    # 获取所有的key为node1的键值对
    keys = {key: value for key, value in ring_data.items() if value == "node1"}
    # 获取备份节点
    new_keys = {}
    chr_instance = ConsistentHashRing()
    # 遍历原始的字典
    for key in keys:
        new_keys[key] = chr_instance.get_next_virtual_node(int(key))
    print("new_keys:")
    print(new_keys)

    # 发送请求进行查询
    for new_key, value in new_keys.items():
        if value== "node2":
            url = f"{base_url2}/total/{author_name}/{start_year}/{end_year}/{new_key}"
        else:
            url = f"{base_url3}/total/{author_name}/{start_year}/{end_year}/{new_key}"
        response = requests.get(url)
        print(response.text)
        total1 += int(response.text.split(",")[0].split(":")[1].strip())

    print("total1:"+str(total1))
    total += total1
    # data_dict = json.loads(response.ring)

try:
    response2 = requests.get(url2)
    print(response2.text)
    total2 = int(response2.text.split(",")[0].split(":")[1].strip())
    total += total2
except Exception as e:
    print(e)
    config_url = f"{base_url3}/config"
    response = requests.get(config_url)
    data_dict = json.loads(response.text)
    ring_data = data_dict.get('ring', {})

    # 获取所有的key为node1的键值对
    keys = {key: value for key, value in ring_data.items() if value == "node2"}
    # 获取备份节点
    new_keys = {}
    chr_instance = ConsistentHashRing()
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
    config_url = f"{base_url1}/config"
    response = requests.get(config_url)
    data_dict = json.loads(response.text)
    ring_data = data_dict.get('ring', {})

    # 获取所有的key为node1的键值对
    keys = {key: value for key, value in ring_data.items() if value == "node3"}
    # 获取备份节点
    new_keys = {}
    chr_instance = ConsistentHashRing()
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

    print("total3:" + str(total1))
    total += total3

print("total number:"+str(total))