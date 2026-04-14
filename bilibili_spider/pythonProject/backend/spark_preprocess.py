import argparse, uuid, subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lit
import datetime
import re
import os
import atexit
from pathlib import Path


def main(hdfs_dir: str, local_out_dir: str, bvid: str):
    spark = SparkSession.builder.master("local[*]").appName("Preprocess").getOrCreate()

    # 从 job 目录读取（你 pipeline_run 上传到 /bilibili_data/日期/job_id/）
    comments_df = spark.read.option("header", True).option("multiLine", True).csv(f"hdfs://localhost:9000{hdfs_dir}/*_comments.csv")
    danmaku_df  = spark.read.option("header", True).option("multiLine", True).csv(f"hdfs://localhost:9000{hdfs_dir}/*_danmaku.csv")
    unrelate_df = spark.read.option("header", False).csv(f"hdfs://localhost:9000{hdfs_dir}/unrelated.csv").toDF("word")

    stopwords = [row["word"] for row in unrelate_df.collect()]
    pattern = "|".join([re.escape(w) for w in stopwords if w])

    # 标准化评论表
    comments_df = comments_df.withColumn("source", lit("comment")).withColumnRenamed("uname", "user")
    if "user" not in comments_df.columns:
        comments_df = comments_df.withColumn("user", lit(None).cast("string"))
    if "content" not in comments_df.columns:
        comments_df = comments_df.withColumn("content", lit(""))
    # 重命名时间列：ctime -> post_time
    if "ctime" in comments_df.columns:
        comments_df = comments_df.withColumnRenamed("ctime", "post_time")
    else:
        comments_df = comments_df.withColumn("post_time", lit(None))
    # 补充弹幕时间相关列为空
    comments_df = comments_df.withColumn("video_time", lit(None)).withColumn("danmaku_time", lit(None))

    # 标准化弹幕表
    danmaku_df = danmaku_df.withColumn("source", lit("danmaku")).withColumn("user", lit(None).cast("string"))
    if "content" not in danmaku_df.columns:
        danmaku_df = danmaku_df.withColumn("content", lit(""))
    # 重命名弹幕时间列：rel_time -> video_time, unix_time -> danmaku_time
    if "rel_time" in danmaku_df.columns:
        danmaku_df = danmaku_df.withColumnRenamed("rel_time", "video_time")
    else:
        danmaku_df = danmaku_df.withColumn("video_time", lit(None))
    if "unix_time" in danmaku_df.columns:
        danmaku_df = danmaku_df.withColumnRenamed("unix_time", "danmaku_time")
    else:
        danmaku_df = danmaku_df.withColumn("danmaku_time", lit(None))
    # 补充评论时间列为空
    danmaku_df = danmaku_df.withColumn("post_time", lit(None))

    # 统一列并合并
    combined_df = comments_df.select("user", "content", "source", "post_time", "video_time", "danmaku_time").unionByName(
        danmaku_df.select("user", "content", "source", "post_time", "video_time", "danmaku_time"),
        allowMissingColumns=True
    )

    cleaned_df = combined_df.withColumn(
        "cleaned_content", regexp_replace(col("content"), pattern, "")
    ).filter(col("cleaned_content").rlike(r"\S"))

    # ✅ 写到本地 single-csv：cleaned_output/{bvid}.csv
    out_dir = Path(local_out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = out_dir / f"{bvid}_tmp"
    final_csv = out_dir / f"{bvid}.csv"

    cleaned_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .option("quoteAll", True) \
        .option("escape", "\"") \
        .csv(str(tmp_dir))

    # Spark 会写成 part-xxx.csv，改名成 final_csv
    import glob, shutil
    parts = glob.glob(str(tmp_dir / "part-*.csv"))

    if not parts:
        raise RuntimeError("Spark 未生成 part-*.csv")

    shutil.move(parts[0], final_csv)
    shutil.rmtree(tmp_dir, ignore_errors=True)

    spark.stop()
    print(f"[spark_preprocess] wrote -> {final_csv}")



if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--hdfs_dir", required=True)
    p.add_argument("--local_out_dir", required=True)
    p.add_argument("--bvid", required=True)
    p.add_argument("--job_id", default=None)
    args = p.parse_args()

    main(args.hdfs_dir, args.local_out_dir, args.bvid)

