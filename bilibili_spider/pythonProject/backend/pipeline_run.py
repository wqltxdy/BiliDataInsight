print("[pipeline_run] module imported")

import os
import sys
import datetime
from pathlib import Path
import subprocess
import json, time

# 这些模块位于同一目录，因此直接导入即可
# 导入 crawl 和 upload 模块（位于 backend 包内）
from .crawl import run_crawl
from backend.upload import _hdfs_cmd
from .upload import upload_dir_to_hdfs

spark_script = Path(__file__).parent / "spark_preprocess.py"
# 项目根目录位于 backend 的上一层，即 pythonProject 根目录
project_root = Path(__file__).resolve().parents[1]
cleaned_dir = project_root / "cleaned_output"
cleaned_dir.mkdir(parents=True, exist_ok=True)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = PROJECT_ROOT / "jobs"
RESULTS_DIR = PROJECT_ROOT / "results"
JOBS_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

def write_status(job_id: str, **kwargs):
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    p = job_dir / "status.json"
    data = {"ts": time.time(), **kwargs}
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def write_result(job_id: str, payload: dict):
    p = RESULTS_DIR / f"{job_id}.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def run_pipeline(bv_or_url: str, cookie: str = None, job_id: str = None):
    """
    同步执行完整的抓取/上传/清洗/情感分析流程，并在每个阶段更新状态文件。

    参数：
      bv_or_url (str): 用户输入的 BV 号或链接
      cookie (str): 可选，覆盖默认请求的 Cookie
      job_id (str): 任务编号，由上层调用生成

    返回：dict 包含 bvid、HDFS 目录、本地清洗输出目录等信息
    """
    # BV号就是输入
    bv = bv_or_url.strip()

    # 记录开始状态
    write_status(job_id, state="running", step="爬取评论弹幕", bv=bv, progress=0)

    # 1) crawl：输出目录
    # 若抓取失败，抛出异常交由上层捕获并标记为 error
    output_dir = run_crawl(bv, job_id=job_id, cookie=cookie)  # output_dir 路径

    # 更新进度：爬取完成
    write_status(job_id, state="running", step="上传HDFS", bv=bv, progress=25)

    # 2) upload：只上传 output_dir 下 csv + unrelated.csv 到 /bilibili_data/<date>/<job_id>/
    today = datetime.date.today().strftime('%Y-%m-%d')
    hdfs_dir = f"/bilibili_data/{today}/{job_id}"

    # 用 upload_dir_to_hdfs（不要用动态扫描）
    upload_dir_to_hdfs(output_dir, hdfs_dir)

    # 上传无关词词库
    cmd = _hdfs_cmd()
    # 项目根目录里的 unrelated.csv
    unrelated = project_root / "unrelated.csv"
    if unrelated.exists():
        subprocess.run(cmd + ["dfs", "-put", "-f", str(unrelated), hdfs_dir], check=True)
    # backend 目录下的 unrelated.csv（如存在）
    unrelated_backend = project_root / "backend" / "unrelated.csv"
    if unrelated_backend.exists():
        subprocess.run(cmd + ["dfs", "-put", "-f", str(unrelated_backend), hdfs_dir], check=True)

    # 更新进度：上传完成
    write_status(job_id, state="running", step="Spark清洗输出", bv=bv, progress=50)

    # 3) spark：写到 pythonProject/cleaned_output/BVxxxx.csv
    cleaned_dir = project_root / "cleaned_output"
    cleaned_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run([
        sys.executable,
        str(spark_script),
        "--hdfs_dir", hdfs_dir,
        "--local_out_dir", str(cleaned_dir.resolve()),
        "--bvid", bv,
        "--job_id", job_id
    ], check=True)

    # 更新进度：清洗完成
    write_status(job_id, state="running", step="情感分析", bv=bv, progress=75)

    # 4) transformers：绝对路径运行
    # sentiment 脚本位于 backend 目录；如不存在则退回到根目录
    sent_script = project_root / "backend" / "transformer_sentiment.py"
    if not sent_script.exists():
        # 部署环境可能不存在 backend 目录，退回 root
        sent_script = project_root / "transformer_sentiment.py"

    input_csv = cleaned_dir / f"{bv}.csv"  # 例如 cleaned_output/BV1GJBTBTEyA.csv
    results_dir = project_root / "results"
    results_dir.mkdir(exist_ok=True)

    out_csv = results_dir / f"{job_id}_sentiment_weighted.csv"

    # 调用 Transformer 情感分析；如果失败则回退到快速分析
    try:
        subprocess.run([
            sys.executable,
            str(sent_script),
            "--input_csv", str(input_csv),
            "--out_csv", str(out_csv),
            "--job_id", job_id
        ], check=True)
        # 更新进度：情感分析完成
        write_status(job_id, state="running", step="话题聚类分析", bv=bv, progress=90)
    except Exception as e:
        # Transformer 模型加载或推理失败，使用快速情感分析回退
        print("[pipeline_run] transformer sentiment failed, fallback to fast sentiment:", e)
        # 写状态，标记为快速分析阶段
        try:
            write_status(job_id, state="running", step="快速情感分析", bv=bv, progress=80, error=str(e))
        except Exception:
            pass
        fast_script = project_root / "backend" / "fast_sentiment.py"
        if not fast_script.exists():
            fast_script = project_root / "fast_sentiment.py"
        # 使用快速分析，不指定 out_csv（仍然写入 job_id_s sentiment_weighted.csv）
        subprocess.run([
            sys.executable,
            str(fast_script),
            "--input_csv", str(input_csv),
            "--out_csv", str(out_csv),
            "--job_id", job_id
        ], check=True)
        # 快速分析结束，进入话题聚类分析
        write_status(job_id, state="running", step="话题聚类分析", bv=bv, progress=90)

    # 5) 话题聚类分析：调用 advanced_analysis.py
    analysis_script = project_root / "backend" / "advanced_analysis.py"
    if not analysis_script.exists():
        analysis_script = project_root / "advanced_analysis.py"
    subprocess.run([
        sys.executable,
        str(analysis_script),
        "--input_csv", str(input_csv),
        "--job_id", job_id
    ], check=True)

    # 更新进度：所有分析完成
    write_status(job_id, state="done", step="完成", bv=bv, progress=100)

    return {"bvid": bv, "hdfs_dir": hdfs_dir, "cleaned_output": str(cleaned_dir.resolve())}


def main():
    import argparse, uuid, traceback
    parser = argparse.ArgumentParser()
    parser.add_argument("--bv", required=True)
    parser.add_argument("--job_id", default=None)
    args = parser.parse_args()

    job_id = args.job_id or uuid.uuid4().hex[:12]
    print("[pipeline_run] BV =", args.bv, "job_id =", job_id)

    try:
        # 这里一定要同步执行，先别用线程
        info = run_pipeline(args.bv, job_id=job_id)
        print("[pipeline_run] DONE:", info)
    except Exception as e:
        # 捕获异常，写入错误状态
        try:
            write_status(job_id, state="error", step="异常", bv=args.bv, error=str(e))
        except Exception:
            pass
        print("[pipeline_run] ERROR:", e)
        print(traceback.format_exc())

if __name__ == "__main__":
    print("[pipeline_run] __main__ entered")
    main()