import copy
from datetime import datetime
import logging
import os
import json
import bisect
import signal
import time
import ast
import mmh3
from flask import Flask
import socket
import threading
import requests
import random
import shutil

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# 读取配置文件
with open("node_config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

# 读取配置文件
with open("gossip.json", "r", encoding="utf-8") as f:
    GOSSIP_CONFIG = json.load(f)


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
        add=GOSSIP_CONFIG["add"],
    ):
        if add == False:
            self.loss = GOSSIP_CONFIG["loss"]
            self.virtuals = virtuals
            self.ring = ring
            self.sorted_keys = sorted_keys
            self.nodes = nodes
            self.node = node
            self.ports = GOSSIP_CONFIG["ports"]
            self.time = GOSSIP_CONFIG["time"]
            self.node_index = CONFIG["nodes"].index(CONFIG["node"])
            self.last_node = CONFIG["nodes"][self.node_index - 1]
            self.next_node_port = self.ports[(self.node_index + 1) % len(self.ports)]
            self.ip = GOSSIP_CONFIG["ip"]
            self.thread = threading.Timer(self.time, self._gossip)
            # 创建一个UDP socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # 绑定到监听的IP和端口
            self.sock.bind((self.ip, self.ports[self.node_index]))
            # 设置为非阻塞模式
            self.sock.setblocking(False)
            self.add = add
            self.count = 0
        else:
            self.loss = GOSSIP_CONFIG["loss"]
            self.add = add
            response = requests.get("http://localhost:8082/config")
            config = json.loads(response.text)
            self.virtuals = CONFIG["virtuals"]
            self.ring = config["ring"]
            self.before_ring = copy.deepcopy(self.ring)
            self.before_ring = config["ring"]
            self.sorted_keys = CONFIG["sorted_keys"]
            self.nodes = config["nodes"]
            self.before_nodes = self.nodes.copy()
            self.node = "node4"
            self.nodes.append(self.node)
            self.ports = GOSSIP_CONFIG["ports"]
            self.before_ports = self.ports.copy()
            self.ports.append(8084)
            self.ip = GOSSIP_CONFIG["ip"]
            self.time = GOSSIP_CONFIG["time"]
            self.node_index = config["nodes"].index("node4")
            self.next_node_port = self.ports[(self.node_index + 1) % len(self.ports)]
            self.thread = threading.Timer(self.time, self._gossip)
            # 从sorted_keys中随机选择75个节点
            self.selected_keys = random.sample(self.sorted_keys, 75)
            # 将这些节点添加到ring中
            for key in self.selected_keys:
                self.ring[key] = self.node
            # 重新排序sorted_keys
            self.sorted_keys.sort()
            # 创建一个UDP socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # 绑定到监听的IP和端口
            self.sock.bind((self.ip, self.ports[self.node_index]))
            # 设置为非阻塞模式
            self.sock.setblocking(False)

            self._initial_data()
            self.current = time.time()

    def get_details(self):
        config = {
            "node": self.node,
            "current": self.current,
            "nodes": self.nodes,
            "ports": self.ports,
            "ring": self.ring,
            "sorted_keys": self.sorted_keys,
        }
        return json.dumps(config)

    def say_hello(self):
        """
        告知其他节点自己的存在
        """
        # 创建一个UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 创建一个消息
        message = {
            "type": "hello",
            "curent":self.current,
            "node": self.node,
            "nodes": self.nodes,
            "ports": self.ports,
            "ring": self.ring,
        }
        # 将字典转换为JSON字符串
        message_json = json.dumps(message)
        # 将JSON字符串转换为字节
        message_bytes = message_json.encode("utf-8")
        # 发送消息
        for port in self.ports:
            if port == self.ports[self.node_index]:
                continue
            sock.sendto(message_bytes, (self.ip, port))
            app.logger.info(f"发送hello消息到{port}")
        # 关闭socket
        sock.close()

    def say_goodbye(self):
        # 直接逆操作
        # 创建一个UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 创建一个消息
        message = {
            "type": "goodbye",
            "node": self.node,
            "nodes": self.before_nodes,
            "ports": self.before_ports,
            "ring": self.before_ring,
        }
        # 将字典转换为JSON字符串
        message_json = json.dumps(message)
        # 将JSON字符串转换为字节
        message_bytes = message_json.encode("utf-8")
        # 发送消息
        for port in self.ports:
            if port == self.ports[self.node_index]:
                continue
            sock.sendto(message_bytes, (self.ip, port))
            app.logger.info(f"发送hello消息到{port}")
        # 关闭socket
        sock.close()
        # 把自己文件夹数据也删了
        shutil.rmtree(f"{self.node}")
        app.logger.info(f"{message}")

    def _initial_data(self):
        # 创建目标目录，如果它不存在
        os.makedirs(f"{self.node}/main_data", exist_ok=True)
        os.makedirs(f"{self.node}/author_index", exist_ok=True)
        os.makedirs(f"{self.node}/date_index", exist_ok=True)
        os.makedirs(f"{self.node}/author_date_index", exist_ok=True)
        for node in self.nodes:
            if node == self.node:
                continue
            data_files = os.listdir(f"{node}/main_data")
            for data_file in data_files:
                # 分割文件名和扩展名
                file_name, file_ext = os.path.splitext(data_file)
                file_name_as_int = int(file_name)
                if file_name_as_int in self.selected_keys:
                    # 如果文件名在selected_keys中，将文件复制到本地
                    shutil.copyfile(
                        f"{node}/main_data/{data_file}",
                        f"{self.node}/main_data/{data_file}",
                    )
                    print(f"从{node}复制{data_file}到{self.node}")
            author_index_files = os.listdir(f"{node}/author_index")
            for author_index_file in author_index_files:
                # 分割文件名和扩展名
                file_name, file_ext = os.path.splitext(author_index_file)
                file_name_as_int = int(file_name)
                if file_name_as_int in self.selected_keys:
                    # 如果文件名在selected_keys中，将文件复制到本地
                    shutil.copyfile(
                        f"{node}/author_index/{author_index_file}",
                        f"{self.node}/author_index/{author_index_file}",
                    )
                    print(f"从{node}复制{author_index_file}到{self.node}")
            date_index_files = os.listdir(f"{node}/date_index")
            for date_index_file in date_index_files:
                # 分割文件名和扩展名
                file_name, file_ext = os.path.splitext(date_index_file)
                file_name_as_int = int(file_name)
                if file_name_as_int in self.selected_keys:
                    # 如果文件名在selected_keys中，将文件复制到本地
                    shutil.copyfile(
                        f"{node}/date_index/{date_index_file}",
                        f"{self.node}/date_index/{date_index_file}",
                    )
                    print(f"从{node}复制{date_index_file}到{self.node}")
            author_date_index_files = os.listdir(f"{node}/author_date_index")
            for author_date_index_file in author_date_index_files:
                # 分割文件名和扩展名
                file_name, file_ext = os.path.splitext(author_date_index_file)
                file_name_as_int = int(file_name)
                if file_name_as_int in self.selected_keys:
                    # 如果文件名在selected_keys中，将文件复制到本地
                    shutil.copyfile(
                        f"{node}/author_date_index/{author_date_index_file}",
                        f"{self.node}/author_date_index/{author_date_index_file}",
                    )
                    print(f"从{node}复制{author_date_index_file}到{self.node}")

    def get_config(self):
        config = {
            "node": self.node,
            "nodes": self.nodes,
            "ports": self.ports,
            "ring": self.ring,
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

    def starter(self):
        app.logger.info("开始gossip")
        self.thread.start()

    def _gossip(self):
        """
        gossip协议
        """
        self.heart_beat()
        # 重新设置定时器
        self.thread = threading.Timer(self.time, self._gossip)
        self.thread.start()

    def heart_beat(self):
        """
        心跳检测
        """
        # 创建一个UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 创建一个心跳消息
        message = {"type": "heartbeat", "node": self.node}
        # 将字典转换为JSON字符串
        message_json = json.dumps(message)
        # 将JSON字符串转换为字节
        message_bytes = message_json.encode("utf-8")

        # 模拟丢包
        if random.random() > self.loss:
            # 发送消息
            sock.sendto(message_bytes, (self.ip, self.next_node_port))
            app.logger.info(f"发送心跳消息到{self.next_node_port}")
        else:
            app.logger.info(f"模拟丢包，未发送心跳消息到{self.next_node_port}")

        # 关闭socket
        sock.close()

        try:
            # 尝试接收一个数据包
            data, addr = self.sock.recvfrom(10240)  # 缓冲区大小为10240字节
            # 将字节转换为JSON字符串
            message_json = data.decode("utf-8")
            # 将JSON字符串转换为字典
            message = json.loads(message_json)
            # 清空缓冲区
            try:
                while True:
                    data_, addr_ = self.sock.recvfrom(10240)
                    msg_json = data_.decode("utf-8")
                    msg = json.loads(msg_json)
                    # 处理优先级高的事件
                    if msg["type"] != "heartbeat":
                        message = msg
                        break
            except BlockingIOError:
                pass
            app.logger.info(f"收到消息:{message}来自{addr}，类型是{message['type']}")
            if (
                message["type"] == "hello"
                or message["type"] == "goodbye"
                or message["type"] == "fail"
            ):
                if message["type"] == "fail" and self.node not in message["nodes"]:
                    # 发现自己被意外排除了马上更新自己时间戳并且 say hello 等待所有服务器回应
                    self.current = time.time()
                    self.say_hello()
                    return
                self.ring = message["ring"]
                self.nodes = message["nodes"]
                self.ports = message["ports"]
                self.node_index = self.nodes.index(self.node)
                self.next_node_port = self.ports[
                    (self.node_index + 1) % len(self.ports)
                ]
                self.last_node = self.nodes[self.node_index - 1]
            else:
                self.count = 0

        except BlockingIOError:
            # 没有数据包可供接收
            app.logger.info("未收到心跳")
            if self.last_node == "node1":
                self.count += 1
            if self.count >= 4:
                self.count = 0
                dt_object = datetime.fromtimestamp(time.time())
                app.logger.error(f"{self.last_node}节点四次心跳检测失败，判断为掉线，掉线时间：{dt_object}")
                self._fail()

    def _fail(self):
        """
        告知别的组服务自己心跳检测的服务器掉线了，并更新信息
        """
        # 创建一个UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        it_before_ports = self.ports.copy()
        if self.last_node in self.nodes:
            index = self.nodes.index(self.last_node)
            self.nodes.remove(self.last_node)
            self.ports.remove(self.ports[index])
        message = {
            "type": "fail",
            "node": self.node,
            "nodes": self.nodes,
            "ports": self.ports,
            "ring": self.ring,
        }
        # 将字典转换为JSON字符串
        message_json = json.dumps(message)
        # 将JSON字符串转换为字节
        message_bytes = message_json.encode("utf-8")
        self.node_index = self.nodes.index(self.node)
        self.next_node_port = self.ports[(self.node_index + 1) % len(self.ports)]
        self.last_node = self.nodes[self.node_index - 1]

        # 发送消息
        for port in it_before_ports:
            if port == self.ports[self.node_index]:
                continue
            sock.sendto(message_bytes, (self.ip, port))
            app.logger.info(f"发送fail消息到{port}")
        # 关闭socket
        sock.close()


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
        if self.hash_ring.add == True:
            self.hash_ring.say_hello()

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


@app.route("/starter")
def starter():
    SERVICE.hash_ring.starter()
    return "Started"


@app.route("/config")
def config():
    return SERVICE.hash_ring.get_config()


def shutdown():
    os.kill(os.getpid(), signal.SIGINT)


@app.route("/goodbye")
def goodbye():
    SERVICE.hash_ring.say_goodbye()

    # 创建一个定时器，0.5秒后执行shutdown函数
    timer = threading.Timer(0.5, shutdown)
    timer.start()

    return "Goodbye"


@app.route("/kill")
def kill():
    dt_object = datetime.fromtimestamp(time.time())
    # 立即停止发送心跳
    SERVICE.hash_ring.thread.cancel()
    app.logger.error(f"{SERVICE.hash_ring.node}节点强制掉线，掉线时间：{dt_object}")
    os.kill(os.getpid(), signal.SIGINT)
    return "Killed"


@app.route("/rejoin")
def rejoin():
    dt_object = datetime.fromtimestamp(time.time())
    app.logger.error(f"{SERVICE.hash_ring.node}节点重新加入，加入时间：{dt_object}")
    SERVICE.hash_ring.say_hello()
    SERVICE.hash_ring.starter()
    return "Rejoined"

@app.route("/details")
def details():
    return SERVICE.hash_ring.get_details()


if __name__ == "__main__":
    app.run(port=SERVICE.hash_ring.ports[SERVICE.hash_ring.node_index], threaded=True)
