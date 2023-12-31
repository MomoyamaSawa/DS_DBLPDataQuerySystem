import os
import json
import bisect
import time
import ast
import mmh3
from flask import Flask

app = Flask(__name__)

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
        self.author_date_index_replica = {}
        self.author_index_replica = {}
        self.date_index_replica = {}
        self._get_author_date_index()
        self._get_author_index()
        self._get_date_index()
        end_time = time.time()
        print(f"初始化主数据索引耗时{end_time - start_time}秒")

    def get_replica_date_index(self, replica):
        index = {}
        path = f"{self.node}_replica\\date_index\\{replica}.json"
        with open(path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        for key, value in data.items():
            if key in index:
                index[key].extend(value)
            else:
                index[key] = value
        return index

    def get_replica_author_index(self, replica):
        index = {}
        path = f"{self.node}_replica\\author_index\\{replica}.json"
        with open(path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        for key, value in data.items():
            if key in index:
                index[key].extend(value)
            else:
                index[key] = value
        return index

    def get_replica_author_date_index(self, replica):
        index = {}
        path = f"{self.node}_replica\\author_date_index\\{replica}.json"
        with open(path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        for key, value in data.items():
            if key in index:
                index[key].extend(value)
            else:
                index[key] = value
        return index

    def get_total_by_author(self, author, replica=0):
        """
        通过作者名获取总数
        """
        if replica == 0:
            author_index = self.author_index
        else:
            author_index = self.get_replica_author_index(replica)
        return len(author_index.get(author, []))

    def get_total_by_date(self, start_date, end_date, replica=0):
        """
        通过日期范围获取总数
        """
        if replica == 0:
            date_index = self.date_index
        else:
            date_index = self.get_replica_date_index(replica)
        total_num = 0
        current_date = start_date
        while current_date <= end_date:
            total_num += len(date_index.get(f"{current_date}", []))
            current_date += 1

        return total_num

    def get_total_by_author_date(self, author, start_date, end_date, replica=0):
        """
        通过作者名和日期获取总数
        """
        if replica == 0:
            author_date_index = self.author_date_index
        else:
            author_date_index = self.get_replica_author_date_index(replica)
        total_num = 0
        current_date = start_date

        while current_date <= end_date:
            total_num += len(author_date_index.get(f"{author}-{current_date}", []))
            current_date += 1

        return total_num

    def _get_author_date_index(self):
        # 遍历文件夹下的所有文件
        walk_dir = f"{self.node}/author_date_index"
        for root, dirs, files in os.walk(walk_dir):
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
        walk_dir = f"{self.node}/author_index"
        for root, dirs, files in os.walk(walk_dir):
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
        walk_dir = f"{self.node}/date_index"
        for root, dirs, files in os.walk(walk_dir):
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

    def get_all_by_全局扫描(self):
        # 通过遍历文件获得所有条数
        total_num = 0
        data_dir = os.path.join(self.node, "main_data")
        virtual_nodes = os.listdir(data_dir)
        for i, virtual_node in enumerate(virtual_nodes):
            with open(
                os.path.join(data_dir, virtual_node), "r", encoding="utf-8"
            ) as file:
                while True:
                    line = file.readline()
                    if not line:
                        break
                    total_num += 1
        return total_num

    def get_by_全局扫描(self, author, start, end):
        # 通过遍历文件获得所有条数
        total_num = 0
        data_dir = os.path.join(self.node, "main_data")
        virtual_nodes = os.listdir(data_dir)
        for i, virtual_node in enumerate(virtual_nodes):
            with open(
                os.path.join(data_dir, virtual_node), "r", encoding="utf-8"
            ) as file:
                while True:
                    line = file.readline()
                    if not line:
                        break
                    # 解析数据
                    values = line.split("|")
                    authors = ast.literal_eval(values[3])
                    date = values[2]
                    if author in authors and start <= int(date.split("-")[0]) <= end:
                        total_num += 1
        return total_num


SERVICE = DataService()


@app.route("/hello")
def home():
    return "Hello, World!"


@app.route("/total/<string:name>/<int:start>/<int:end>")
def total(name, start, end):
    start_time = time.perf_counter()

    if start == 0 and end == 0 and name != "any":
        total_num = SERVICE.get_total_by_author(name)
    elif start == 0 and end != 0 and name != "any":
        total_num = SERVICE.get_total_by_author_date(name, 1936, end)
    elif end == 0 and start != 0 and name != "any":
        total_num = SERVICE.get_total_by_author_date(name, start, 2024)
    elif start != 0 and end != 0 and name != "any":
        if start < 1936:
            start = 1936
        if end > 2024:
            end = 2024
        total_num = SERVICE.get_total_by_author_date(name, start, end)
    elif start != 0 and end == 0 and name == "any":
        total_num = SERVICE.get_total_by_date(start, 2024)
    elif start == 0 and end != 0 and name == "any":
        total_num = SERVICE.get_total_by_date(1936, end)
    elif start != 0 and end != 0 and name == "any":
        total_num = SERVICE.get_total_by_date(start, end)
    else:
        total_num = SERVICE.get_total_by_date(1936, 2024)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    return f"Total: {total_num}, Query Time: {elapsed_time:.7f} seconds"


@app.route("/total/<string:name>/<int:start>/<int:end>/<string:replica>")
def total_replica(name, start, end, replica):
    replica = int(replica)
    start_time = time.perf_counter()

    if start == 0 and end == 0 and name != "any":
        total_num = SERVICE.get_total_by_author(name, replica)
    elif start == 0 and end != 0 and name != "any":
        total_num = SERVICE.get_total_by_author_date(name, 1936, end, replica)
    elif end == 0 and start != 0 and name != "any":
        total_num = SERVICE.get_total_by_author_date(name, start, 2024, replica)
    elif start != 0 and end != 0 and name != "any":
        if start < 1936:
            start = 1936
        if end > 2024:
            end = 2024
        total_num = SERVICE.get_total_by_author_date(name, start, end, replica)
    elif start != 0 and end == 0 and name == "any":
        total_num = SERVICE.get_total_by_date(start, 2024, replica)
    elif start == 0 and end != 0 and name == "any":
        total_num = SERVICE.get_total_by_date(1936, end, replica)
    elif start != 0 and end != 0 and name == "any":
        total_num = SERVICE.get_total_by_date(start, end, replica)
    else:
        total_num = SERVICE.get_total_by_date(1936, 2024, replica)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    return f"Total: {total_num}, Query Time: {elapsed_time:.7f} seconds"


@app.route("/test/all")
def test_all():
    start_time = time.perf_counter()
    total_num = SERVICE.get_all_by_全局扫描()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    return f"Total: {total_num}, Query Time: {elapsed_time:.7f} seconds"


@app.route("/test/<string:name>/<int:start>/<int:end>")
def test(name, start, end):
    start_time = time.perf_counter()
    total_num = SERVICE.get_by_全局扫描(name, start, end)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    return f"Total: {total_num}, Query Time: {elapsed_time:.7f} seconds"


if __name__ == "__main__":
    app.run(port=8080, threaded=True)
