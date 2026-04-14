<template>
  <div class="page">
    <!-- 顶部导航栏：仿 bilibili 风格 -->
    <nav class="navbar">
      <div class="navbar-left">
        <!-- 平台 Logo 区域 -->
        <span class="platform-logo">📺</span>
        <span class="platform-name">弹幕评论分析平台</span>
        <!-- 历史记录下拉菜单 -->
        <div
            class="history-container"
            @mouseenter="handleHistoryEnter"
            @mouseleave="handleHistoryLeave"
        >
          <span class="nav-link history-link">历史记录</span>
          <ul v-if="showHistory && history.length > 0" class="history-dropdown">
            <li
              v-for="item in history"
              :key="item.jobId"
              @click="loadHistory(item)"
            >
              {{ item.bv }}
            </li>
          </ul>
        </div>
      </div>
      <div class="navbar-right">
        <input
          v-model="bv"
          placeholder="请输入视频 BV 号，如 BV1xxxx"
          :disabled="running"
        />
        <button :disabled="!bv || running" @click="start">
          {{ running ? "分析中..." : "开始分析" }}
        </button>
      </div>
    </nav>

    <div v-if="jobId" class="status">
      <p>当前状态：{{ state }}</p>
      <p>当前步骤：{{ step }}</p>
      <p>进度：{{ progress }}%</p>
    </div>

    <div v-if="error" class="error">
      ❌ {{ error }}
    </div>

    <!-- 只有拿到真实结果，才渲染图 -->
    <SentimentViz
      v-if="showViz"
      :counts="counts"
      :avgWeighted="avgWeighted"
      :timeline="timeline"
      :burstPoints="burstPoints"
      :anomalies="anomalies"
      :topWords="topWords"
      :differenceScore="differenceScore"
      :clusters="clusters"
    />

    <!-- DeepSeek 智能分析聊天框 -->
    <div v-if="showDeepseek" class="deepseek-panel" :class="{ minimized: deepseekMinimized }">
      <div class="deepseek-header">
        <div class="deepseek-title">
          DeepSeek 智能分析
          <span class="deepseek-badge">{{ statusText }}</span>
        </div>
        <div class="deepseek-actions">
          <button class="deepseek-iconbtn" @click="deepseekMinimized = !deepseekMinimized">
            {{ deepseekMinimized ? '+' : '-' }}
          </button>
        </div>
      </div>

      <div v-show="!deepseekMinimized" class="deepseek-body" ref="deepseekBodyRef">
        <div
            v-for="(msg, idx) in deepseekMessages"
            :key="idx"
            class="deepseek-row"
            :class="msg.role === 'user' ? 'user' : 'assistant'"
        >
          <div class="deepseek-bubble">{{ msg.content }}</div>
        </div>

        <div v-if="deepseekMessages.length === 0" class="deepseek-row assistant">
          <div class="deepseek-bubble">正在分析中…我会在结果出来后给出可视化解读与优化建议。</div>
        </div>
      </div>

      <div v-show="!deepseekMinimized" class="deepseek-footer">
    <textarea
        v-model="deepseekQuestion"
        class="deepseek-input"
        placeholder="继续追问：比如“爆点在哪里？标题怎么改？”"
        @keydown.enter.exact.prevent="sendDeepseek"
        @keydown.enter.shift.exact.stop
    ></textarea>
        <button class="deepseek-send" :disabled="sendingDeepseek" @click="sendDeepseek">
          {{ sendingDeepseek ? '发送中…' : '发送' }}
        </button>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, computed, nextTick } from "vue"
import SentimentViz from "../components/SentimentViz.vue"

const API = "http://127.0.0.1:8000"

const bv = ref("")
const running = ref(false)

const jobId = ref("")
const state = ref("")
const step = ref("")
const progress = ref(0)

const counts = ref({})
const avgWeighted = ref({})
// 新增：分析结果
const timeline = ref({})
const burstPoints = ref({})
const anomalies = ref({})
const topWords = ref({})
const differenceScore = ref(null)
const clusters = ref({})
const showViz = ref(false)

const error = ref("")

// 深度分析模块：聊天记录与状态
const showDeepseek = ref(false)
const deepseekMessages = ref([])
const deepseekQuestion = ref("")
const deepseekMinimized = ref(false)

// 历史记录：存储已完成分析的视频及其对应 jobId
const history = ref([])
const showHistory = ref(false);
let historyTimer = null; // 新增：用于控制延迟的定时器

const sendingDeepseek = ref(false)
const deepseekBodyRef = ref(null)

const deepseekStatus = ref("idle")

// 用你现有的 status（你页面里应该有 status 对象/状态文本）来映射
const statusText = computed(() => {
  switch (deepseekStatus.value) {
    case "idle": return "未开始"
    case "analyzing": return "分析中"
    case "ready": return "可追问"
    case "sending": return "发送中"
    case "error": return "异常"
    default: return "未知"
  }
})


// 新增：鼠标进入容器时，清除定时器并显示
const handleHistoryEnter = () => {
  if (historyTimer) clearTimeout(historyTimer);
  showHistory.value = true;
};

// 新增：鼠标离开容器时，延迟 300 毫秒再隐藏
const handleHistoryLeave = () => {
  historyTimer = setTimeout(() => {
    showHistory.value = false;
  }, 300); // 300ms 足够你把鼠标从“文字”移动到“下方菜单”了
};


// 初始化历史记录
try {
  const stored = localStorage.getItem('history')
  if (stored) {
    history.value = JSON.parse(stored)
  }
} catch (e) {
  // ignore parsing errors
  history.value = []
}

let timer = null

function reset() {
  state.value = ""
  step.value = ""
  progress.value = 0
  counts.value = {}
  avgWeighted.value = {}
  // 重置新增分析结果
  timeline.value = {}
  burstPoints.value = {}
  anomalies.value = {}
  topWords.value = {}
  differenceScore.value = null
  clusters.value = {}
  showViz.value = false
  error.value = ""
  showDeepseek.value = false
  deepseekMessages.value = []
  deepseekQuestion.value = ""

}

function openDeepseekPending() {
  deepseekStatus.value = "analyzing"
  state.value = "running"
  step.value = "正在分析"
  progress.value = 0

  showDeepseek.value = true
  deepseekQuestion.value = ""
  deepseekMessages.value = [
    { role: "assistant", content: "正在分析中…我会在结果出来后给出可视化解读与优化建议。" }
  ]
}

async function scrollDeepseekToBottom() {
  await nextTick()
  const el = deepseekBodyRef.value
  if (!el) return
  el.scrollTop = el.scrollHeight
}

async function sendDeepseek() {
  if (!jobId.value) return
  const q = (deepseekQuestion.value || "").trim()
  if (!q) return

  // 1) 先把用户消息塞进去
  deepseekMessages.value.push({role: "user", content: q})
  deepseekQuestion.value = ""
  await scrollDeepseekToBottom()

  // 2) 调后端
  sendingDeepseek.value = true
  deepseekStatus.value = "sending"
  try {
    const resp = await fetch(`http://127.0.0.1:8000/api/deepseek/${jobId.value}`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({question: q})
    })
    const data = await resp.json()

    const msgs = data?.messages
    if (Array.isArray(msgs) && msgs.length > 0) {
      // ✅ 后端返回的是“助手消息列表”，我们把它 append 到聊天里
      for (const m of msgs) {
        deepseekMessages.value.push(m)
      }
      await scrollDeepseekToBottom()
    }
    deepseekStatus.value = "ready"
  } catch (e) {
    deepseekMessages.value.push({
      role: "assistant",
      content: `（请求失败）${String(e)}`
    })
    await scrollDeepseekToBottom()
    deepseekStatus.value = "error"
  } finally {
    sendingDeepseek.value = false
  }
}

async function start() {
  reset()
  running.value = true

  showDeepseek.value = true
  try {
    // 1️⃣ 启动 pipeline
    const r = await fetch(`${API}/api/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ bv: bv.value })
    })
// 开始分析调用 api
    if (!r.ok) throw new Error("启动任务失败")

    const data = await r.json()
    jobId.value = data.job_id
    state.value = "running"
    step.value = "准备启动"
    progress.value = 0

    openDeepseekPending()

    // 2️⃣ 立刻轮询一次（避免“第一次像假的”）
    await poll()

    timer = setInterval(poll, 1200)

  } catch (e) {
    running.value = false
    error.value = e.message
  }
}

async function poll() {
  try {
    const r = await fetch(`${API}/api/status/${jobId.value}`)
    if (!r.ok) throw new Error("状态查询失败")

    const s = await r.json()

    state.value = s.state
    step.value = s.step
    progress.value = s.progress ?? progress.value

    if (s.state === "done" && s.has_result) {
      clearInterval(timer)
      timer = null
      await fetchResult()
    }

    if (s.state === "error") {
      throw new Error("后端任务失败")
    }
  } catch (e) {
    clearInterval(timer)
    timer = null
    running.value = false
    error.value = e.message
  }
}

async function fetchResult() {
  const r = await fetch(`${API}/api/result/${jobId.value}`)
  if (!r.ok) throw new Error("结果获取失败")

  const res = await r.json()
  counts.value = res.label_counts
  avgWeighted.value = res.label_weighted_mean
  // 传播节奏与爆点分析、差异分析、异常识别结果
  timeline.value = res.timeline || {}
  burstPoints.value = res.burst_points || {}
  anomalies.value = res.anomalies || {}
  topWords.value = res.top_words || {}
  differenceScore.value = res.difference_score ?? null

  clusters.value = res.clusters || {}

  showViz.value = true
  running.value = false

  // 将当前分析加入历史记录
  addToHistory()

  // 初始化深度分析模块
  await initDeepseek()
}

// 添加历史记录：如果记录中不存在相同 bv 的 jobId，则添加
function addToHistory() {
  if (!bv.value || !jobId.value) return
  // 检查是否已存在相同记录
  const exists = history.value.some(item => item.bv === bv.value && item.jobId === jobId.value)
  if (!exists) {
    history.value.push({ bv: bv.value, jobId: jobId.value })
    // 保存到 localStorage
    try {
      localStorage.setItem('history', JSON.stringify(history.value))
    } catch (e) {
      // ignore storage errors
    }
  }
}

// 从历史记录加载结果
async function loadHistory(item) {
  reset()
  // 隐藏下拉菜单
  showHistory.value = false
  // 设置当前 bv 和 jobId
  bv.value = item.bv
  jobId.value = item.jobId

  state.value = "running"
  step.value = "准备启动"
  progress.value = 0
  openDeepseekPending()
  deepseekStatus.value = "ready"

  // 清空错误并重置显示
  error.value = ""
  state.value = "done"
  step.value = "已完成"
  progress.value = 100
  running.value = false
  // 直接拉取历史结果并显示
  try {
    await fetchResult()
  } catch (e) {
    error.value = e.message
  }
}

// 初始化 deepseek 智能分析：获取初步建议
async function initDeepseek() {
  if (!jobId.value) return
  deepseekStatus.value = "analyzing"

  const resp = await fetch(`http://127.0.0.1:8000/api/deepseek/${jobId.value}`)
  const data = await resp.json()
  const msgs = data?.messages

  if (!Array.isArray(msgs) || msgs.length === 0) return

  const onlyTip =
    msgs.length === 1 &&
    typeof msgs[0]?.content === "string" &&
    msgs[0].content.includes("暂无深度分析结果")

  if (onlyTip) return

  deepseekMessages.value = msgs
  deepseekStatus.value = "ready"
}


// 发送提问给 deepseek 接口并更新聊天记录
async function askDeepseek() {
  const question = deepseekQuestion.value.trim()
  if (!question) return
  // 将用户问题加入消息列表
  deepseekMessages.value.push({ role: 'user', content: question })
  deepseekQuestion.value = ""
  try {
    const r = await fetch(`${API}/api/deepseek/${jobId.value}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question })
    })
    if (r.ok) {
      const data = await r.json()
      // 假设后端返回 messages 数组或单条信息
      if (Array.isArray(data.messages)) {
        data.messages.forEach(msg => deepseekMessages.value.push(msg))
      } else if (data.message) {
        deepseekMessages.value.push({ role: 'assistant', content: data.message })
      }
    } else {
      deepseekMessages.value.push({ role: 'assistant', content: '深度分析提问失败' })
    }
  } catch (e) {
    deepseekMessages.value.push({ role: 'assistant', content: '深度分析接口调用失败' })
  }
}


</script>

<style scoped>
.page {
  width: 90%;
  max-width: 1400px;
  margin: 0 auto;
  padding: 24px;
}

/* 顶部导航栏 */
.navbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  background: #ffffff;
  padding: 12px 30px;
  border-radius: 25px;
  border: 1px solid rgba(0,0,0,0.06);
  box-shadow: 0 2px 4px rgba(0,0,0,0.04);
  margin-bottom: 18px;
}

.navbar-left {
  display: flex;
  align-items: center;
  gap: 50px;
}

.platform-logo {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 10px;
}

.platform-name {
  white-space: nowrap;
  word-break: keep-all;
  font-weight: 700;
  font-size: 16px;
  letter-spacing: 0.3px;
}

.navbar-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.nav-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  height: 34px;
  padding: 0 12px;
  border-radius: 10px;
  border: 1px solid rgba(0,0,0,0.08);
  background: #fff;
  cursor: pointer;
  font-weight: 600;
  transition: all 0.2s ease;
}

.nav-link:hover {
  background: #f6f7fb;
}

/* 历史记录下拉 */
.history-container {
  position: relative;
  display: inline-block;
}

.history-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  height: 34px;
  padding: 0 12px;
  border-radius: 10px;
  border: 1px solid rgba(0,0,0,0.08);
  background: #fff;
  cursor: pointer;
  font-weight: 600;
  transition: all 0.2s ease;
}

.history-link:hover {
  background: #f6f7fb;
}

.history-dropdown {
  position: absolute;
  top: 42px;
  left: -45%;
  width: 200%;
  box-sizing: border-box;
  max-height: 360px;
  overflow-y: auto;
  background: #fff;
  border: 1px solid rgba(0,0,0,0.08);
  border-radius: 12px;
  box-shadow: 0 10px 24px rgba(0,0,0,0.10);
  padding: 8px;
  z-index: 20;
}

.history-dropdown ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.history-dropdown li {
  padding: 10px 10px;
  border-radius: 10px;
  cursor: pointer;
  line-height: 1.3;
}

.history-dropdown li:hover {
  background: #f5f5f5;
}

/* DeepSeek 聊天框 */
.deepseek-panel {
  position: fixed;
  right: 18px;
  bottom: 18px;
  width: 480px;
  height: 650px;
  border-radius: 18px;
  overflow: hidden;
  border: 1px solid rgba(0,0,0,0.08);
  background: #fff;
  box-shadow: 0 20px 40px rgba(0,0,0,0.18);
  display: flex;
  flex-direction: column;
  z-index: 30;
  transition: height 0.2s ease;
}

.deepseek-panel.minimized {
  height: 52px;
  overflow: hidden;
}

.deepseek-header {
  height: 52px;
  padding: 0 12px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-weight: 600;
  background: linear-gradient(180deg, #fbfbfb 0%, #f5f5f5 100%);
  border-bottom: 1px solid rgba(0,0,0,0.06);
}

.deepseek-title {
  display: inline-flex;
  align-items: center;
  gap: 10px;
}

.deepseek-badge {
  height: 22px;
  padding: 0 10px;
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  font-size: 12px;
  border: 1px solid rgba(0,0,0,0.10);
  background: #ffffff;
}

.deepseek-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.deepseek-iconbtn {
  height: 32px;
  padding: 0 12px;
  border-radius: 10px;
  border: 1px solid rgba(0,0,0,0.10);
  background: #fff;
  cursor: pointer;
  font-weight: 600;
}

.deepseek-body {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
  background: #fafafa;
}

.deepseek-row {
  display: flex;
  margin: 10px 0;
}

.deepseek-row.user {
  justify-content: flex-end;
}

.deepseek-row.assistant {
  justify-content: flex-start;
}

.deepseek-bubble {
  max-width: 78%;
  padding: 10px 12px;
  border-radius: 14px;
  font-size: 13px;
  line-height: 1.45;
  border: 1px solid rgba(0,0,0,0.06);
  background: #fff;
  word-break: break-word;
  white-space: pre-wrap;
  text-align: left;
}

.user .deepseek-bubble {
  background: #eef5ff;
  border-color: rgba(47,124,255,0.18);
}

.assistant .deepseek-bubble {
  background: #ffffff;
  border-color: rgba(0,0,0,0.08);
}

.deepseek-footer {
  padding: 10px;
  border-top: 1px solid rgba(0,0,0,0.06);
  background: #fff;
  display: flex;
  gap: 8px;
  align-items: flex-end;
}

.deepseek-input {
  flex: 1;
  min-height: 36px;
  max-height: 92px;
  resize: none;
  padding: 8px 10px;
  border-radius: 12px;
  border: 1px solid rgba(0,0,0,0.12);
  outline: none;
  font-size: 13px;
}

.deepseek-send {
  height: 36px;
  padding: 0 14px;
  border-radius: 12px;
  border: 0;
  background: #2f7cff;
  color: #fff;
  cursor: pointer;
  font-weight: 600;
}

.deepseek-send:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

</style>
