"""
deepseek_module.py

This module integrates DeepSeek 智能分析功能。它读取在 ``results``
目录下生成的分析结果 JSON，并通过 DeepSeek 模型生成摘要和优化建议。用户也可以
在页面右侧的聊天框中继续提问。

``DEEPSEEK_API_KEY`` 和 ``DEEPSEEK_API_ENDPOINT`` 常量需要由使用者
自行替换成有效的 API 密钥和接口地址。默认模型为 ``deepseek-chat-v3.2``。

主要接口:

``analyze(job_id: str) -> list[dict]``
    加载指定 job 的分析结果，生成初步摘要和优化建议，并调用 DeepSeek API
    返回一条助手信息列表。

``chat(job_id: str, question: str) -> list[dict]``
    根据历史分析摘要和用户提问，调用 DeepSeek API 返回回答。
"""

import os
import json
from pathlib import Path
import requests

# Define project root relative to this file
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "results"

# =========================================================================
# DeepSeek 配置
# -------------------------------------------------------------------------
# TODO: 请将下面的 API key 和 endpoint 替换为您自己的，确保能够正常调用
# DeepSeek v3.2 模型。如果您希望使用其他模型，请修改 DEEPSEEK_MODEL。

# API 密钥（必须替换）
DEEPSEEK_API_KEY = "sk-98bf3699b6444223b06fe0ede9fd1bb6"

# API 端点（无需修改，除非官方文档另有说明）
DEEPSEEK_API_ENDPOINT = "https://api.deepseek.com/v1/chat/completions"

# 默认模型名称，可根据需要调整。DeepSeek v3.2 版本推荐使用 deepseek-chat-v3.2。
DEEPSEEK_MODEL = "deepseek-chat"

def _load_result(job_id: str) -> dict:
    """Load the analysis result JSON for a job id."""
    result_path = RESULTS_DIR / f"{job_id}.json"
    if not result_path.exists():
        raise FileNotFoundError(f"Result for job_id {job_id} not found")
    return json.loads(result_path.read_text(encoding="utf-8"))

def _generate_summary(data: dict) -> str:
    """Generate a simple textual summary of analysis results."""
    parts = []
    # Label counts summary
    label_counts = data.get("label_counts", {})
    if label_counts:
        counts_str = ", ".join(f"{k}:{v}" for k, v in label_counts.items())
        parts.append(f"情感分布为 {counts_str}。")
    # Difference score
    diff = data.get("difference_score")
    if diff is not None:
        parts.append(f"话题差异度为 {diff:.2f}。")
    # Top words summary
    top_words = data.get("top_words", {})
    if top_words:
        words_summary = []
        for src, words in top_words.items():
            tops = [w for w, c in words[:5]]
            words_summary.append(f"{src} 的高频词包括 {', '.join(tops)}")
        parts.append("；".join(words_summary) + "。")
    # Clusters summary
    clusters = data.get("clusters", {})
    if clusters:
        cluster_parts = []
        for src, cdata in clusters.items():
            counts = cdata.get("counts", [])
            if counts:
                cluster_parts.append(f"{src} 共有 {len(counts)} 个话题")
        if cluster_parts:
            parts.append("，".join(cluster_parts) + "。")
    return " ".join(parts) if parts else "暂无摘要。"

def _call_deepseek_api(messages: list[dict], temperature: float = 0.7) -> str:
    """调用 DeepSeek API 并返回助手回复文本。

    参数 ``messages`` 需为符合 OpenAI/DeepSeek 接口规范的消息列表，包括
    ``system``、``user``、``assistant`` 等角色。``temperature`` 控制随机性。

    如果 API 密钥未替换或请求失败，将返回相应错误提示。
    """
    # 检查密钥和端点
    api_key = DEEPSEEK_API_KEY
    endpoint = DEEPSEEK_API_ENDPOINT
    if not api_key or api_key.startswith("sk-xxxxxxxx"):
        return "[DeepSeek API 未配置，请在 deepseek_module.py 中设置 DEEPSEEK_API_KEY]"
    if not endpoint:
        return "[DeepSeek API 端点未配置，请在 deepseek_module.py 中设置 DEEPSEEK_API_ENDPOINT]"
    # 构建请求
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": messages,
        "temperature": temperature,
    }
    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        # 根据 DeepSeek API 返回结构提取回复内容
        choices = data.get("choices")
        if choices and isinstance(choices, list):
            return choices[0].get("message", {}).get("content", "")
        # fallback to 'answer' field
        return data.get("answer", "")
    except Exception as e:
        return f"[调用 DeepSeek API 失败: {e}]"

def analyze(job_id: str) -> list[dict]:
    """Return initial DeepSeek suggestions as a list of chat messages.

    Each message is a dict with 'role' ('assistant') and 'content'.
    """
    data = _load_result(job_id)
    summary = _generate_summary(data)
    # 构建系统提示和用户请求，详细描述任务
    system_prompt = (
        "你是一名资深的视频数据运营分析师。你需要根据弹幕和评论的分析结果，"
        "为UP主总结传播情况、观众情绪、话题差异，并给出优化建议。"
        "请注意你会和UP主在一个布局大小有限的聊框内进行交流，所以尽量不要出现长段话语，页面也不支持表情，不需要过多的提示符号。注意分段，由于聊天框很小，如果不进行分段，可能对于UP来说阅读有点困难，还有减少'#*-_='这样类似的符号，不利于阅读"
    )
    user_prompt = (
        "以下是分析摘要：" + summary + "。\n请根据以上内容进行深度思考，"
        "给出完整的分析总结和具体的创作优化建议。"
    )
    # 调用 DeepSeek API
    answer = _call_deepseek_api([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    return [{"role": "assistant", "content": answer}]

def chat(job_id: str, question: str) -> list[dict]:
    """Handle a follow-up question in DeepSeek chat.

    Returns a list of messages from the assistant.
    """
    data = _load_result(job_id)
    summary = _generate_summary(data)
    system_prompt = (
        "你是一名哔哩哔哩内容分析助手。根据视频的弹幕与评论分析摘要回答UP主的问题，"
        "并提供专业建议。"
    )
    user_prompt = (
        "分析摘要：" + summary + "。\nUP主的问题：" + question
    )
    answer = _call_deepseek_api([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    return [{"role": "assistant", "content": answer}]