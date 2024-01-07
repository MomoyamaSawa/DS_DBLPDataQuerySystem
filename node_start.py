import os
import json
import bisect
import ast
import time
import mmh3

# 读取配置文件
with open("node_config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)


class HashIndex:
    """
    给每个物理节点建立三个哈希索引
    """

    def __init__(self, node_name):
        self.node_name = node_name
        # 索引暂存
        self.author_index = {}
        self.date_index = {}
        self.author_date_index = {}

    def build_index(self):
        data_dir = os.path.join(self.node_name, "main_data")
        virtual_nodes = os.listdir(data_dir)
        # 创建文件夹
        os.makedirs(os.path.join(self.node_name, "author_index"), exist_ok=True)
        os.makedirs(os.path.join(self.node_name, "date_index"), exist_ok=True)
        os.makedirs(os.path.join(self.node_name, "author_date_index"), exist_ok=True)
        start_time = time.time()
        for i, virtual_node in enumerate(virtual_nodes):
            with open(
                os.path.join(data_dir, virtual_node), "r", encoding="utf-8"
            ) as file:
                # 去掉文件名的扩展名
                virtual_node_name, _ = os.path.splitext(virtual_node)
                while True:
                    # 获取当前文件指针位置
                    pos = file.tell()
                    line = file.readline()
                    if not line:
                        break
                    # 解析数据
                    values = line.split("|")
                    authors = ast.literal_eval(values[3])
                    date = values[2]
                    # 更新索引
                    year = date.split("-")[0]
                    if year not in self.date_index:
                        self.date_index[year] = []
                    self.date_index[year].append((virtual_node_name, pos))
                    for author in authors:
                        if author not in self.author_index:
                            self.author_index[author] = []
                        self.author_index[author].append((virtual_node_name, pos))
                        # 添加作者和日期的索引
                        author_date = f"{author}-{year}"
                        if author_date not in self.author_date_index:
                            self.author_date_index[author_date] = []
                        self.author_date_index[author_date].append(
                            (virtual_node_name, pos)
                        )
            # 处理一个节点保存一次索引
            self._save_index(virtual_node_name)
            # 清理索引
            self.author_index.clear()
            self.date_index.clear()
            self.author_date_index.clear()
            # 每处理10个虚拟节点打印一次进度
            if (i + 1) % 10 == 0:
                elapsed_time = time.time() - start_time
                print(f"Progress: {i+1} nodes, elapsed time: {elapsed_time} seconds")

    def _save_index(self, virtual_node):
        with open(
            os.path.join(self.node_name, "author_index", f"{virtual_node}.json"),
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(self.author_index, file)
        with open(
            os.path.join(self.node_name, "date_index", f"{virtual_node}.json"),
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(self.date_index, file)
        with open(
            os.path.join(self.node_name, "author_date_index", f"{virtual_node}.json"),
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(self.author_date_index, file)


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

    def set(self, node, nodes, ring, sorted_keys):
        self.node = node
        self.nodes = nodes
        self.ring = ring
        self.sorted_keys = sorted_keys


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

    def replica(self):
        for node in self.nodes:
            os.makedirs(f"{node}_replica\\main_data", exist_ok=True)
            os.makedirs(f"{node}_replica\\author_index", exist_ok=True)
            os.makedirs(f"{node}_replica\\date_index", exist_ok=True)
            os.makedirs(f"{node}_replica\\author_date_index", exist_ok=True)
        for virtual_node_key in self.sorted_keys:
            # 检查仅当虚拟节点所在的物理节点是当前节点时才进行复制
            if self.ring[str(virtual_node_key)] != self.node:
                continue
            next_node = self.get_next_virtual_node(virtual_node_key)
            os.system(
                f"copy {self.node}\\main_data\\{virtual_node_key}.data {next_node}_replica\\main_data\\{virtual_node_key}.data"
            )
            os.system(
                f"copy {self.node}\\author_index\\{virtual_node_key}.json {next_node}_replica\\author_index\\{virtual_node_key}.json"
            )
            os.system(
                f"copy {self.node}\\date_index\\{virtual_node_key}.json {next_node}_replica\\date_index\\{virtual_node_key}.json"
            )
            os.system(
                f"copy {self.node}\\author_date_index\\{virtual_node_key}.json {next_node}_replica\\author_date_index\\{virtual_node_key}.json"
            )


if __name__ == "__main__":
    hash_ring = ConsistentHashRing()
    total_start_time = time.time()
    # 建立索引
    hash_index = HashIndex(hash_ring.node)
    hash_index.build_index()
    # 将数据备份到副本节点
    print("Replicating data...")
    hash_ring.replica()
    total_end_time = time.time()
    print(f"\nTotal elapsed time: {total_end_time-total_start_time} seconds.")
