# backend/api_server.py
import json
import os
import subprocess
import uuid
import webbrowser
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

# Import deepseek module for intelligent analysis
try:
    from . import deepseek_module
except Exception:
    import deepseek_module

# 项目根目录：backend 的上一层
PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = PROJECT_ROOT / "jobs"
RESULTS_DIR = PROJECT_ROOT / "results"
JOBS_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

app = FastAPI()

# 允许前端跨域（开发期）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RunReq(BaseModel):
    bv: str

def job_paths(job_id: str):
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    status_path = job_dir / "status.json"
    log_path = job_dir / "run.log"
    result_path = RESULTS_DIR / f"{job_id}.json"
    return job_dir, status_path, log_path, result_path

def write_status(status_path: Path, **kwargs):
    data = {"ts": __import__("time").time(), **kwargs}
    status_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

@app.post("/api/run")
def run_pipeline(req: RunReq):
    bv = req.bv.strip()
    if not bv.startswith("BV"):
        raise HTTPException(400, "BV 号格式不正确")

    job_id = uuid.uuid4().hex[:12]
    job_dir, status_path, log_path, result_path = job_paths(job_id)
    write_status(status_path, state="queued", step="准备启动", bv=bv, job_id=job_id)

    # 用当前环境 python 跑 pipeline，使用包名方式加载 backend.pipeline_run
    cmd = [
        os.environ.get("PYTHON", "") or str(Path(os.sys.executable)),
        "-m", "backend.pipeline_run",
        "--bv", bv,
        "--job_id", job_id,
    ]

    with open(log_path, "w", encoding="utf-8") as f:
        # 后台进程：让 API 立即返回
        subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=f,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )

    write_status(status_path, state="running", step="任务已启动", bv=bv, job_id=job_id)
    return {"job_id": job_id}

@app.get("/api/status/{job_id}")
def status(job_id: str):
    _, status_path, log_path, result_path = job_paths(job_id)
    if not status_path.exists():
        raise HTTPException(404, "job_id 不存在")
    data = json.loads(status_path.read_text(encoding="utf-8"))

    # 附带最后几行日志（方便前端显示“正在进行哪一步”）
    tail = ""
    if log_path.exists():
        try:
            lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            tail = "\n".join(lines[-25:])
        except Exception:
            tail = ""
    data["log_tail"] = tail
    data["has_result"] = result_path.exists()
    return data

@app.get("/api/result/{job_id}")
def result(job_id: str):
    _, _, _, result_path = job_paths(job_id)
    if not result_path.exists():
        raise HTTPException(404, "结果尚未生成")
    return json.loads(result_path.read_text(encoding="utf-8"))

# DeepSeek intelligent analysis endpoints
@app.get("/api/deepseek/{job_id}")
def deepseek_analyze(job_id: str):
    """Return initial suggestions for the given job using DeepSeek analysis.

    如果结果文件不存在，则返回一条提示信息而不是 404 错误，避免前端重复弹错。
    """
    try:
        messages = deepseek_module.analyze(job_id)
        return {"messages": messages}
    except FileNotFoundError:
        # 如果结果文件不存在，返回空提示信息，前端可显示“无法获取深度分析建议”
        return {"messages": [{"role": "assistant", "content": "暂无深度分析结果，请稍后重试。"}]}
    except Exception as e:
        # 其它异常返回 500
        raise HTTPException(500, f"DeepSeek 分析失败: {e}")


class ChatReq(BaseModel):
    question: str

@app.post("/api/deepseek/{job_id}")
def deepseek_chat(job_id: str, req: ChatReq):
    """Answer a follow-up question for the given job using DeepSeek chat.

    如果结果文件不存在，则返回提示信息而不是 404 错误。
    """
    try:
        messages = deepseek_module.chat(job_id, req.question)
        return {"messages": messages}
    except FileNotFoundError:
        return {"messages": [{"role": "assistant", "content": "暂无深度分析结果，请稍后重试。"}]}
    except Exception as e:
        raise HTTPException(500, f"DeepSeek 对话失败: {e}")

if __name__ == "__main__":
    import uvicorn
    # 在启动时自动打开浏览器访问 /docs
    docs_url = "http://localhost:8000/docs"
    print(f"API docs will be opened at: {docs_url}")
    webbrowser.open(docs_url)

    # uvicorn.run(app, host="0.0.0.0", port=8000)
    # 使用 reload 以便代码修改后自动重启（开发模式）
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
