import argparse
import json
import traceback
from pathlib import Path

import pandas as pd
from transformers import pipeline

def load_unrelated_words(path: str = 'unrelated.csv'):
    """加载无关词库。如果文件不存在/读取失败则返回空集合（避免直接报错）。"""
    try:
        return set(pd.read_csv(path, header=None)[0].dropna().astype(str).tolist())
    except Exception:
        return set()

def calculate_weight(text, unrelated_words):
    if any(word in text for word in unrelated_words):
        return 0.4
    return 1.0

def split_text(text: str, max_chars: int = 400):
    """把长文本切成多段（字符级），每段长度 max_chars。"""
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)
    text = text.strip()
    if not text:
        return [""]
    return [text[i:i + max_chars] for i in range(0, len(text), max_chars)]

def aggregate_segments(seg_results, mode="max_strength"):
    """
    把多段情绪结果聚合成一条评论的最终 label/score
    mode:
      - "max_strength": 取 score 最大的那段作为最终（适合“情绪激烈”筛选）
      - "avg_score": score 平均，并用平均得分对应的“主标签”近似（更平滑）
    """
    if not seg_results:
        return {"label": "UNKNOWN", "score": 0.0, "seg_n": 0}

    if mode == "avg_score":
        avg = sum(r["score"] for r in seg_results) / len(seg_results)
        # label 用“最强段”的 label（简单且可解释）
        best = max(seg_results, key=lambda x: x["score"])
        return {"label": best["label"], "score": float(avg), "seg_n": len(seg_results)}

    # 默认：最大强度（推荐）
    best = max(seg_results, key=lambda x: x["score"])
    return {"label": best["label"], "score": float(best["score"]), "seg_n": len(seg_results)}

def sentiment_analysis(input_csv: str, out_csv: str, job_id: str = None):
    """对单个清洗后的 CSV 做情感分析。

    注意：pipeline_run 会传入 --input_csv（例如 cleaned_output/BVxxxx.csv），
    这里必须只分析这个文件，避免把历史 CSV 也拼进来导致耗时/错乱。
    """
    input_path = Path(input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"找不到 input_csv: {input_csv}")
    df = pd.read_csv(input_path, engine='python', on_bad_lines='skip')

    # 2) 规范列名
    df.columns = df.columns.astype(str).str.strip()

    # 3) 自动找文本列（你也可以直接写死成你自己的列名）
    candidate_cols = ["content", "评论内容", "comment", "text", "message", "弹幕", "danmu", "danmaku"]
    text_col = next((c for c in candidate_cols if c in df.columns), None)
    if text_col is None:
        raise KeyError(f"找不到文本列。当前列名为: {df.columns.tolist()}")

    # 4) 保留原文用于前端展示
    df[text_col] = df[text_col].fillna("").astype(str)
    df["content_raw"] = df[text_col]

    # 5) 权重（基于原文算也可以）
    unrelated_words = load_unrelated_words()
    df["weight"] = df["content_raw"].apply(lambda x: calculate_weight(x, unrelated_words))

    # 6) pipeline（这里开 truncation 防止某段仍 >512 token）
    classifier = pipeline(
        "sentiment-analysis",
        model="bardsai/finance-sentiment-zh-fast",
        truncation=True,
        max_length=512
    )

    # 7) 对每条评论：分段 -> 批量跑 -> 聚合
    #    说明：这里按“每条评论内部批处理”，实现最清晰；数据很大时可再优化为全局批处理。
    final_labels = []
    final_scores = []
    seg_counts = []

    for text in df["content_raw"].tolist():
        segments = split_text(text, max_chars=400)
        seg_res = classifier(segments)  # 一条评论的多段结果
        agg = aggregate_segments(seg_res, mode="max_strength")  # 推荐 max_strength
        final_labels.append(agg["label"])
        final_scores.append(agg["score"])
        seg_counts.append(agg["seg_n"])

    df["label"] = final_labels
    df["score"] = final_scores
    df["seg_n"] = seg_counts

    # 8) 加权得分（这里用聚合后的 score）
    df["weighted_score"] = df["score"] * df["weight"]

    # 9) 保存到 pipeline_run 指定的 out_csv
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("使用的文本列:", text_col)
    print("情感分布：")
    print(df["label"].value_counts())
    print("各类平均加权得分：")
    print(df.groupby("label")["weighted_score"].mean())
    print("长文本分段统计（seg_n）：")
    print(df["seg_n"].describe())

    summary = {
        "label_counts": df["label"].value_counts().to_dict(),
        "label_weighted_mean": df.groupby("label")["weighted_score"].mean().to_dict(),
        "rows": int(len(df)),
    }
    # 汇总 JSON 写到 out_csv 同目录（通常是项目根目录的 results/）
    results_dir = out_path.resolve().parent
    results_dir.mkdir(parents=True, exist_ok=True)
    result_path = results_dir / (f"{job_id}.json" if job_id else "latest.json")

    result_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("情感分析结果已写入：", result_path)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_csv", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--job_id", required=False)
    args = ap.parse_args()

    try:
        sentiment_analysis(
            input_csv=args.input_csv,
            out_csv=args.out_csv,
            job_id=args.job_id
        )
    except Exception as e:
        print("[transformer_sentiment] ERROR:", repr(e))
        traceback.print_exc()
        raise
