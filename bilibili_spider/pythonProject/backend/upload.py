import os
import subprocess
import datetime
from pathlib import Path
import shutil

def _hdfs_cmd():
    """
    返回可执行的 hdfs 命令列表前缀，例如：
    ["hdfs"] 或 ["D:\\hadoop\\bin\\hdfs.cmd"]
    """
    # 1) 优先用环境变量 HADOOP_HOME
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        cand = Path(hadoop_home) / "bin" / "hdfs.cmd"
        if cand.exists():
            return [str(cand)]
        cand = Path(hadoop_home) / "bin" / "hdfs.exe"
        if cand.exists():
            return [str(cand)]

    # 2) 其次用用户自定义 HDFS_BIN（你可以自己设置成绝对路径）
    hdfs_bin = os.environ.get("HDFS_BIN")
    if hdfs_bin and Path(hdfs_bin).exists():
        return [hdfs_bin]

    # 3) 最后尝试 PATH
    found = shutil.which("hdfs")
    if found:
        return [found]

    raise FileNotFoundError(
        "找不到 hdfs 命令。请在系统环境变量设置 HADOOP_HOME，"
        "或设置 HDFS_BIN=...\\hdfs.cmd，或把 hadoop\\bin 加入 PATH。"
    )

def hdfs_upload_dynamic(hdfs_dir: str = None):
    today = datetime.date.today().strftime('%Y-%m-%d')
    if hdfs_dir is None:
        hdfs_dir = f'/bilibili_data/{today}'

    output_dir = './output'
    files_to_upload = []

    # 修正路径格式，兼容 Windows/Linux
    if os.path.exists(output_dir):
        for file in os.listdir(output_dir):
            if file.startswith("bilibili_") and file.endswith(".csv"):
                file_path = str(Path(output_dir) / file).replace("\\", "/")
                files_to_upload.append(file_path)

    # 查找项目根目录下的无关词词库文件（unrelated 开头）
    for file in os.listdir('.'):
        if file.startswith('unrelated'):
            files_to_upload.append(str(Path(file)))
            break

    # 创建 HDFS 目录
    subprocess.run(f"hdfs dfs -mkdir  -p {hdfs_dir}", shell=True)

    for file in files_to_upload:
        if os.path.exists(file):
            print(f"Uploading {file} to {hdfs_dir}/...")
            subprocess.run(f"hdfs dfs -put -f {file} {hdfs_dir}/", shell=True)
        else:
            print(f"[跳过] 文件 {file} 不存在")

    print("上传完成")
    subprocess.run(f"hdfs dfs -ls {hdfs_dir}", shell=True)

def upload_dir_to_hdfs(local_dir: str, hdfs_dir: str):
    local_dir = str(local_dir)
    cmd = _hdfs_cmd()

    # mkdir
    subprocess.run(cmd + ["dfs", "-mkdir", "-p", hdfs_dir], check=True)

    # 上传目录下所有 csv
    for p in Path(local_dir).glob("*.csv"):
        subprocess.run(cmd + ["dfs", "-put", "-f", str(p), hdfs_dir], check=True)
        print(f"Uploading {p} to {hdfs_dir}/...")

    print("上传完成")
    subprocess.run(cmd + ["dfs", "-ls", hdfs_dir], check=True)

if __name__ == '__main__':
    hdfs_upload_dynamic()