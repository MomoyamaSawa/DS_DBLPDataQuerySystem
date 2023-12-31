import os
import json
import shutil

# 读取配置文件
with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

CURRENT_DIR = os.getcwd()

if __name__ == "__main__":
    for node in CONFIG["nodes"]:
        # 删除节点文件夹及其内容
        shutil.rmtree(f"{CURRENT_DIR}/{node}", ignore_errors=True)
