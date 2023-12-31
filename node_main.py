import os
import json
import bisect
import time
import mmh3

# 读取配置文件
with open("node_config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)


class ConsistentHashRing:
    """
    一致性哈希环，分布式存储
    """

    def __init__(
        self,
        node=CONFIG["node"],
        nodes=CONFIG["nodes"],
        virtuals=CONFIG["virtuals"],
        ring=CONFIG["ring"],
        sorted_keys=CONFIG["sorted_keys"],
    ):
        self.virtuals = virtuals
        self.ring = ring
        self.sorted_keys = sorted_keys
        self.nodes = nodes
        self.node = node

    def get_config(self):
        config = {
            "node": self.node,
            "nodes": self.nodes,
            "virtuals": self.virtuals,
            "ring": self.ring,
            "sorted_keys": self.sorted_keys,
        }
        return json.dumps(config)

    def get_next_virtual_node(self, virtual_node_key):
        """
        寻找冗余容错节点
        """
        idx = bisect.bisect(self.sorted_keys, virtual_node_key)
        current_node = self.ring[str(virtual_node_key)]
        while True:
            if idx == len(self.sorted_keys):
                # 如果没有比给定节点哈希值大的节点，返回哈希环上的第一个节点
                idx = 0
            next_node = self.ring[str(self.sorted_keys[idx])]
            # 如果下一个虚拟节点所在的物理节点和当前物理节点不同，返回该虚拟节点
            # 这边只能实现一个节点掉了，如果要实现多个节点掉了还能恢复的话这边的判断要判断是否和已存节点列表相同
            if next_node != current_node:
                return next_node
            # 否则，继续向后找
            idx += 1

    def _hash(self, key):
        return mmh3.hash(key)


class DataService:
    """
    数据节点服务
    """

    def __init__(self, name=CONFIG["node"]) -> None:
        self.node = name
        self.hash_ring = ConsistentHashRing()
        # 初始化主数据索引
        start_time = time.time()
        self.author_date_index = {}
        self.author_index = {}
        self.date_index = {}
        self._get_author_date_index()
        self._get_author_index()
        self._get_date_index()
        end_time = time.time()
        print(f"初始化主数据索引耗时{end_time - start_time}秒")

    def _get_author_date_index(self):
        # 遍历文件夹下的所有文件
        for root, dirs, files in os.walk(f"{self.node}/author_date_index"):
            for file in files:
                # 文件路径
                file_path = os.path.join(root, file)

                # 读取JSON文件的内容
                with open(file_path, "r", encoding="utf-8") as json_file:
                    data = json.load(json_file)

                # 将内容添加到self.author_date_index字典中
                for key, value in data.items():
                    if key in self.author_date_index:
                        self.author_date_index[key].extend(value)
                    else:
                        self.author_date_index[key] = value

    def _get_author_index(self):
        # 遍历文件夹下的所有文件
        for root, dirs, files in os.walk(f"{self.node}/author_index"):
            for file in files:
                # 文件路径
                file_path = os.path.join(root, file)

                # 读取JSON文件的内容
                with open(file_path, "r", encoding="utf-8") as json_file:
                    data = json.load(json_file)

                # 将内容添加到self.author_date_index字典中
                for key, value in data.items():
                    if key in self.author_index:
                        self.author_index[key].extend(value)
                    else:
                        self.author_index[key] = value

    def _get_date_index(self):
        # 遍历文件夹下的所有文件
        for root, dirs, files in os.walk(f"{self.node}/date_index"):
            for file in files:
                # 文件路径
                file_path = os.path.join(root, file)

                # 读取JSON文件的内容
                with open(file_path, "r", encoding="utf-8") as json_file:
                    data = json.load(json_file)

                # 将内容添加到self.author_date_index字典中
                for key, value in data.items():
                    if key in self.date_index:
                        self.date_index[key].extend(value)
                    else:
                        self.date_index[key] = value


if __name__ == "__main__":
    data_service = DataService()
    print(data_service.author_date_index)
    print(data_service.author_index)
    print(data_service.date_index)
