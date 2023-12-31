import os
import bisect
import json
import time
from lxml import etree
import mmh3

# 读取配置文件
with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

CURRENT_DIR = os.getcwd()


class ConsistentHashRing:
    """
    一致性哈希环，分布式存储
    """

    def __init__(self, nodes=CONFIG["nodes"], virtuals=CONFIG["virtual_node_num"]):
        self.virtuals = virtuals
        self.ring = dict()
        self.sorted_keys = []
        self.nodes = nodes

        if self.nodes:
            for node in self.nodes:
                self.add_node(node)

    def add_node(self, node):
        # 本地针对物理阶段创建一个文件夹和里面的主节点数据文件夹
        os.mkdir(f"{CURRENT_DIR}/{node}")
        os.mkdir(f"{CURRENT_DIR}/{node}/main_data")
        for i in range(0, self.virtuals):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            self.sorted_keys.append(key)
            # 每个虚拟节点都对应一个文件
            with open(f"{CURRENT_DIR}/{node}/main_data/{key}.data", "wb"):
                pass

        # 重新排序，保证节点顺序
        self.sorted_keys.sort()

    # def _get_node(self, virtual_node):
    #     """
    #     这个函数的作用是根据给定的虚拟节点在一致性哈希环中找到相应的节点。
    #     现在它会返回一个节点列表，包括原始节点和副本节点。
    #     """
    #     # 获取原始节点
    #     original_node = self.ring[virtual_node]
    #     # # 获取副本节点
    #     # idx = (idx + 1) % len(self.sorted_keys)
    #     # replica_node = self.ring[self.sorted_keys[idx]]
    #     return original_node

    def get_virtual_node(self, key):
        hashed_key = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hashed_key)
        if idx == len(self.sorted_keys):
            idx = 0

        # 获取哈希值和虚拟节点
        return hashed_key, self.sorted_keys[idx]

    def _hash(self, key):
        return mmh3.hash(key)

    def insert_data_batch(self, batch):
        # 将数据插入到对应的文件中
        for virtual_node, node_data_list in batch.items():
            node = self.ring[virtual_node]
            with open(
                f"{CURRENT_DIR}/{node}/main_data/{virtual_node}.data", "ab"
            ) as file:
                for data in node_data_list:
                    # 提取数据的值
                    values = list(data.values())
                    # 使用'|'连接值，并添加换行符
                    line = "|".join(map(str, values)) + "\n"
                    # 写入文件
                    file.write(line.encode("utf-8"))


if __name__ == "__main__":
    # 初始化节点
    hash_ring = ConsistentHashRing()

    # 打开XML文件并读取数据
    data_batch = {}
    current_data = {}
    count = 0

    # 记录开始时间
    start_time = time.time()
    last_time = start_time
    last_solved_time = start_time

    # 解析XML文件
    for event, elem in etree.iterparse(
        CONFIG["xml_data_path"], events=("start",), load_dtd=True
    ):
        # 只处理<article>标签
        if elem.tag != "article":
            continue

        # 创建一个新的字典来存储数据
        current_data = {
            "key": elem.attrib.get("key"),
            "hash": None,
            "mdate": elem.attrib.get("mdate"),
            "author": [],
        }

        # 找到所有的<author>标签，并将它们的文本添加到current_data字典中
        for item_author in elem.findall("author"):
            if item_author.text is not None:
                current_data["author"].append(item_author.text)

        # 将current_data字典插入data_batch列表中
        item_hash, item_node = hash_ring.get_virtual_node(current_data["key"])
        current_data["hash"] = item_hash
        if item_node not in data_batch:
            data_batch[item_node] = []
        data_batch[item_node].append(current_data)
        count += 1

        if count % 50000 == 0:
            current_time = time.time()
            print(
                f"Time elapsed for last 50000 articles: {current_time - last_time} seconds."
            )
            last_time = current_time

        if count % 500000 == 0:
            hash_ring.insert_data_batch(data_batch)
            data_batch = []
            current_time = time.time()
            print(
                f"Solved {count} articles. Time elapsed for last 500000 articles: {current_time - last_solved_time} seconds."
            )
            last_solved_time = current_time
            last_time = current_time
            data_batch = {}

        # 清除当前元素，以节省内存
        elem.clear()

    # 打印总数
    print(f"\nTotal articles: {count}")
    # 插入剩余的数据
    if data_batch:
        hash_ring.insert_data_batch(data_batch)

    # 记录结束时间并计算总用时
    end_time = time.time()
    print(f"Total time elapsed: {end_time - start_time} seconds.")
    print("\nPlease wait for garbage collection, do not close the program...")
