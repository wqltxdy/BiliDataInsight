"""
advanced_analysis.py
====================

本脚本在 sentiment 分析之后执行，用于对爬取和清洗后的弹幕/评论数据进行深入分析，包括：

1. 传播节奏与爆点检测
2. 弹幕 × 评论差异分析
3. 异常行为识别
4. 话题 / 观点聚类分析

使用方式：

    python advanced_analysis.py --input_csv cleaned_output/BVxxxx.csv --job_id <job_id>

该脚本会读取指定的 CSV 文件，计算上述分析指标，并将结果合并到
results/<job_id>.json 文件中（如果该文件不存在，则新建）。

依赖：sentence-transformers、scikit-learn、pandas、numpy
"""

import argparse
import json
import math
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
try:
    # 优先尝试导入 SentenceTransformer，如不可用则回退到 None
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:
    SentenceTransformer = None  # type: ignore
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

# --------- 停用词加载 ---------
# 尝试从与本脚本同目录下的 stop_words.txt 加载停用词。
# 如果文件不存在，则使用一个简短的默认停用词列表。
_stopwords_file_candidate = Path(__file__).resolve().parent / "stop_words.txt"
if _stopwords_file_candidate.exists():
    try:
        with open(_stopwords_file_candidate, "r", encoding="utf-8") as f:
            # 过滤空行和注释
            STOPWORDS_SET = {line.strip() for line in f if line.strip() and not line.startswith('#')}
    except Exception:
        STOPWORDS_SET = set()
else:
    # 默认常见停用词，可在需要时补充
    STOPWORDS_SET = {
        "的", "是", "了", "不", "在", "有", "就", "都", "而", "及", "与", "着", "或", "被", "让",
        "给", "于", "等", "吧", "啊", "呀", "嘛", "啦", "咯", "哦", "哈", "呢", "喔", "嘿",
        "不了", "这个", "那个", "什么", "就是", "但是", "没有", "还是", "一个", "我们", "你们",
        "他们", "她们", "它们", "因为", "所以", "然后", "如果", "还是", "这样", "那么", "就是",
    }

# --------- 全局嵌入模型加载 ---------
# 优先尝试加载 sentence-transformers 模型；若包或模型不可用，则设置为 None，
# 聚类时将回退到基于 TF-IDF 的方法。
if SentenceTransformer is not None:
    EMBED_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
    try:
        EMBED_MODEL = SentenceTransformer(EMBED_MODEL_NAME)
    except Exception:
        try:
            EMBED_MODEL = SentenceTransformer("BAAI/bge-m3")
        except Exception:
            EMBED_MODEL = None
else:
    EMBED_MODEL = None


def tokenize(text: str):
    """
    对中英文混合文本进行简单分词并去除停用词。

    该实现的目标是在没有外部分词库的情况下尽量提取有意义的词语，避免将
    常见的语法助词和单个汉字作为高频词：

      * 连续的英文字母或数字视为一个 token。
      * 连续的汉字视为一个 token，而不是逐字切分。
      * 去除所有长度为 1 的汉字 token（例如“的”、“是”等），同时过滤一批常见
        停用词（包括部分长度为 2 的无意义组合，如“不了”）。

    参数：
      text (str): 原始文本

    返回：
      List[str]: 过滤后的 token 列表
    """
    import re
    # 提取连续的英文字母/数字串或连续的汉字串
    raw_tokens = re.findall(r'[A-Za-z0-9]+|[\u4e00-\u9fa5]+', text)
    tokens: list[str] = []
    # 使用全局加载的停用词集合
    STOPWORDS = STOPWORDS_SET
    for tok in raw_tokens:
        # 过滤纯空白
        if not tok or not tok.strip():
            continue
        # 如果是汉字串
        if re.fullmatch(r'[\u4e00-\u9fa5]+', tok):
            # 去除长度为1的汉字（单字无明显意义）
            if len(tok) <= 1:
                continue
            # 去除停用词
            if tok in STOPWORDS:
                continue
            tokens.append(tok)
        else:
            # 英文或数字串，保留大于等于1长度
            tokens.append(tok)
    return tokens


def compute_hist_and_anomalies(times: pd.Series, bins: int = 20):
    """
    根据时间序列生成直方图，并识别突增点和异常点。

    参数：
      times (pd.Series): 时间序列（应为数值类型）
      bins (int): 直方图分箱数

    返回：
      hist_info (dict): {"bins": bin_edges, "counts": counts}
      bursts (list[int]): 计数 > mean + std 的箱索引
      anomalies (list[int]): 计数 > mean + 2*std 的箱索引
    """
    if times is None or len(times) == 0:
        return {"bins": [], "counts": []}, [], []
    # 去除 NaN
    times = times.dropna()
    if len(times) == 0:
        return {"bins": [], "counts": []}, [], []
    hist, bin_edges = np.histogram(times, bins=bins)
    mean = hist.mean()
    std = hist.std() if hist.std() > 0 else 1e-9
    bursts = [int(i) for i, c in enumerate(hist) if c > mean + std]
    anomalies_list = [int(i) for i, c in enumerate(hist) if c > mean + 2 * std]
    return {"bins": bin_edges.tolist(), "counts": hist.tolist()}, bursts, anomalies_list


def compute_clusters(texts, max_clusters: int = 3, max_samples: int = 1000):
    """
    对给定文本列表进行聚类，并返回每类的文本数量和高频词。

    为了控制运行时间：
      1. 采用较轻量的预训练模型 (MiniLM)，模型在模块导入时就初始化并缓存。
      2. 对于过大的文本列表，只随机抽取前 max_samples 条进行聚类。

    参数：
      texts (List[str]): 文本列表
      max_clusters (int): 最大聚类数（默认 3）
      max_samples (int): 聚类时最多使用的样本数量

    返回：
      dict: {"counts": [n1, n2, ...], "top_words": {"0": [...], "1": [...], ...}}
    """
    import random
    n = len(texts)
    if n == 0:
        return {"counts": [], "top_words": {}}
    # 抽样以控制聚类规模
    if n > max_samples:
        texts_sample = texts[:max_samples]  # 也可以使用 random.sample(texts, max_samples)
    else:
        texts_sample = texts
    m = len(texts_sample)
    # 生成嵌入向量：优先使用预训练模型，如不可用则回退到 TF-IDF
    if EMBED_MODEL is not None:
        embeddings = EMBED_MODEL.encode(texts_sample, batch_size=32, show_progress_bar=False, normalize_embeddings=True)
    else:
        # 使用 TfidfVectorizer 将文本转为特征向量，适用于英文+中文混合
        # 为减少训练时间，限制特征数量
        vectorizer = TfidfVectorizer(max_features=5000)
        embeddings = vectorizer.fit_transform(texts_sample).toarray()
    # 经验规则：根据样本数决定聚类数，至少 2 类
    if m <= 20:
        num_clusters = 2
    else:
        num_clusters = int(math.sqrt(m) // 2 + 1)
        num_clusters = max(2, min(max_clusters, num_clusters))
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(embeddings)
    counts = [int((labels == i).sum()) for i in range(num_clusters)]
    clusters_top = {}
    for cluster_id in range(num_clusters):
        cluster_texts = [t for t, l in zip(texts_sample, labels) if l == cluster_id]
        tokens = []
        for ct in cluster_texts:
            tokens.extend(tokenize(ct))
        counter = Counter(tokens)
        top_tokens = [w for w, c in counter.most_common(10) if w.strip()]
        clusters_top[str(cluster_id)] = top_tokens
    return {"counts": counts, "top_words": clusters_top}


def analyze_behavior(input_csv: str, job_id: str):
    # 读取清洗后的 CSV
    df = pd.read_csv(input_csv)
    # 确保存在必要列
    for col in ["source", "content", "post_time", "video_time", "danmaku_time"]:
        if col not in df.columns:
            df[col] = np.nan
    # 转换时间列为数值
    df["post_time"] = pd.to_numeric(df["post_time"], errors="coerce")
    df["video_time"] = pd.to_numeric(df["video_time"], errors="coerce")

    # 分离评论和弹幕
    comments_df = df[df["source"] == "comment"].copy()
    danmaku_df = df[df["source"] == "danmaku"].copy()

    timeline = {}
    burst_points = {}
    anomalies = {}

    # 评论传播节奏
    if len(comments_df) > 0 and comments_df["post_time"].notna().any():
        rel_times = comments_df["post_time"].dropna()
        # 统一以分钟为单位相对时间（相对最早时间）
        min_post_time = rel_times.min()
        rel_times = (rel_times - min_post_time) / 60.0
        hist_info, bursts_c, anomalies_c = compute_hist_and_anomalies(rel_times)
        timeline["comments"] = hist_info
        burst_points["comments"] = bursts_c
        anomalies["comments"] = anomalies_c
    else:
        timeline["comments"] = {"bins": [], "counts": []}
        burst_points["comments"] = []
        anomalies["comments"] = []

    # 弹幕传播节奏
    if len(danmaku_df) > 0 and danmaku_df["video_time"].notna().any():
        rel_times_d = danmaku_df["video_time"].dropna()
        hist_info, bursts_d, anomalies_d = compute_hist_and_anomalies(rel_times_d)
        timeline["danmaku"] = hist_info
        burst_points["danmaku"] = bursts_d
        anomalies["danmaku"] = anomalies_d
    else:
        timeline["danmaku"] = {"bins": [], "counts": []}
        burst_points["danmaku"] = []
        anomalies["danmaku"] = []

    # 差异分析：高频词
    top_words = {"comments": [], "danmaku": []}
    # 评论高频词
    if len(comments_df) > 0:
        comments_text = " ".join(comments_df["content"].dropna().astype(str).tolist())
        tokens_c = tokenize(comments_text)
        counter_c = Counter(tokens_c)
        top_words["comments"] = [(w, int(c)) for w, c in counter_c.most_common(10) if w.strip()]
    # 弹幕高频词
    if len(danmaku_df) > 0:
        danmaku_text = " ".join(danmaku_df["content"].dropna().astype(str).tolist())
        tokens_d = tokenize(danmaku_text)
        counter_d = Counter(tokens_d)
        top_words["danmaku"] = [(w, int(c)) for w, c in counter_d.most_common(10) if w.strip()]
    # 差异度得分：1 - intersection/union
    set_c = set([w for w, _ in top_words["comments"]])
    set_d = set([w for w, _ in top_words["danmaku"]])
    union = set_c | set_d
    if union:
        intersection = set_c & set_d
        difference_score = 1.0 - (len(intersection) / len(union))
    else:
        difference_score = None

    # 聚类分析（话题/观点）
    clusters = {}
    try:
        clusters["comments"] = compute_clusters(comments_df["content"].dropna().astype(str).tolist())
    except Exception as e:
        clusters["comments"] = {"counts": [], "top_words": {}}
        print("[advanced_analysis] comments clustering error:", e)
    try:
        clusters["danmaku"] = compute_clusters(danmaku_df["content"].dropna().astype(str).tolist())
    except Exception as e:
        clusters["danmaku"] = {"counts": [], "top_words": {}}
        print("[advanced_analysis] danmaku clustering error:", e)

    # 读取 sentiment 分析结果 JSON
    results_dir = Path("results")
    # 若结果目录不存在则创建
    if not results_dir.exists():
        results_dir.mkdir(parents=True, exist_ok=True)
    result_json_path = results_dir / f"{job_id}.json"
    if result_json_path.exists():
        with open(result_json_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
    else:
        summary = {}
    # 更新/新增字段
    summary["timeline"] = timeline
    summary["burst_points"] = burst_points
    summary["anomalies"] = anomalies
    summary["top_words"] = top_words
    summary["difference_score"] = difference_score
    summary["clusters"] = clusters

    # 覆盖写入 JSON
    result_json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[advanced_analysis] 分析结果已写入：{result_json_path}")


def main():
    parser = argparse.ArgumentParser(description="Compute advanced analyses on Bilibili comments and danmaku")
    parser.add_argument("--input_csv", required=True, help="Path to cleaned CSV (from spark_preprocess)")
    parser.add_argument("--job_id", required=True, help="Job ID to locate result JSON")
    args = parser.parse_args()
    analyze_behavior(args.input_csv, args.job_id)


if __name__ == "__main__":
    main()