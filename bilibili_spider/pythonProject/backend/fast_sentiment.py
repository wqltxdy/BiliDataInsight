"""
fast_sentiment.py
==================

本脚本提供一个轻量级的情绪分析实现，用于在无法加载大型 Transformer 模型时
作为回退方案。它不依赖网络下载预训练模型，仅通过简单的词典匹配统计
积极/消极词汇的出现次数来判断情绪倾向。

使用方式：

    python fast_sentiment.py --input_csv cleaned_output/BVxxxx.csv --out_csv output.csv --job_id <job_id>

参数说明：
  --input_csv: 清洗后的 CSV 文件路径（必须包含评论文本列）
  --out_csv:   输出 CSV 路径，将附加 label、score、seg_n、weight、weighted_score 等列
  --job_id:    任务编号，用于将汇总结果写入 results/<job_id>.json

情绪标签规则：
  - 若正面词数量 > 负面词数量，则 label="positive"，score=1.0
  - 若负面词数量 > 正面词数量，则 label="negative"，score=1.0
  - 若二者相等且都为 0，则 label="neutral"，score=0.0
  - 否则 label="neutral"，score=0.5

加权得分：score × weight，其中 weight 来源于 transformer_sentiment.py 中的 calculate_weight。

结果输出：除汇总 JSON 外，out_csv 中还会输出原文列 content_raw 和聚合后的情绪结果，便于前端展示。

注意：此算法仅作为紧急回退方案，准确度有限，不建议替代深度模型。
"""

import argparse
import json
from pathlib import Path
from typing import Optional
import pandas as pd


# 轻量级情绪词典（可以根据需要扩充）
POSITIVE_WORDS = {
    "好", "喜欢", "爱", "满意", "支持", "赞", "不错", "棒", "开心", "快乐", "精彩", "厉害"
}
NEGATIVE_WORDS = {
    "差", "不好", "失望", "讨厌", "垃圾", "生气", "差劲", "烂", "糟糕", "烦", "糟", "愤怒"
}


def load_unrelated_words(path: str = 'unrelated.csv'):
    """
    加载无关词词库，用于计算权重。如果文件不存在，则返回空集合。
    """
    try:
        df = pd.read_csv(path, header=None)
        return set(df[0].dropna().astype(str).tolist())
    except Exception:
        return set()


def calculate_weight(text: str, unrelated_words: set) -> float:
    """
    如果文本中包含任意无关词，则返回较低的权重（0.4），否则返回 1.0。
    """
    if any(word in text for word in unrelated_words):
        return 0.4
    return 1.0


def classify_text(text: str) -> tuple[str, float]:
    """
    对一段文本进行简单的情绪判定，返回 (label, score)。

    规则：比较正面词和负面词数量。若正面多则 positive，负面多则 negative，
    若都为 0 则 neutral，若数量相等且非 0 则 neutral。
    返回的 score 表示情绪强度，固定为 1.0 或 0.5/0.0。
    """
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)
    # 统计正面词和负面词出现次数
    pos_count = sum(text.count(w) for w in POSITIVE_WORDS)
    neg_count = sum(text.count(w) for w in NEGATIVE_WORDS)
    if pos_count > neg_count:
        return "positive", 1.0
    if neg_count > pos_count:
        return "negative", 1.0
    # 所有其他情况（没有情绪词，或正负情绪词一样多）都视为中性
    return "neutral", 0.5


def fast_sentiment_analysis(input_csv: str, out_csv: str, job_id: Optional[str] = None):
    """
    读取清洗后的 CSV，执行简单情绪分析并保存结果。
    同时将汇总信息写入 results/<job_id>.json。
    """
    df = pd.read_csv(input_csv, engine='python', on_bad_lines='skip')
    # 标准化列名
    df.columns = df.columns.astype(str).str.strip()
    # 确定文本列
    candidate_cols = [
        "content", "评论内容", "comment", "text", "message", "弹幕", "danmu", "danmaku"
    ]
    text_col = next((c for c in candidate_cols if c in df.columns), None)
    if text_col is None:
        raise KeyError(f"找不到文本列。当前列名为: {df.columns.tolist()}")
    df[text_col] = df[text_col].fillna("").astype(str)
    df["content_raw"] = df[text_col]
    # 加载无关词库，计算权重
    unrelated_words = load_unrelated_words()
    df["weight"] = df["content_raw"].apply(lambda x: calculate_weight(x, unrelated_words))
    labels = []
    scores = []
    seg_counts = []
    for text in df["content_raw"].tolist():
        label, score = classify_text(text)
        labels.append(label)
        scores.append(score)
        seg_counts.append(1)
    df["label"] = labels
    df["score"] = scores
    df["seg_n"] = seg_counts
    df["weighted_score"] = df["score"] * df["weight"]
    # 保存结果 CSV
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    # 生成汇总信息
    summary = {
        "label_counts": df["label"].value_counts().to_dict(),
        "label_weighted_mean": df.groupby("label")["weighted_score"].mean().to_dict(),
        "rows": int(len(df)),
    }
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    result_path = results_dir / f"{job_id}.json" if job_id else results_dir / "latest.json"
    result_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[fast_sentiment_analysis] 情感分析结果已写入：", result_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", required=True)
    parser.add_argument("--out_csv", required=True)
    parser.add_argument("--job_id", required=False)
    args = parser.parse_args()
    fast_sentiment_analysis(args.input_csv, args.out_csv, args.job_id)


if __name__ == "__main__":
    main()