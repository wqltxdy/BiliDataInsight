<template>
  <section class="viz">
    <div class="viz-grid">
      <!-- 卡片1：情感分布 & 平均得分 -->
      <div class="viz-panel">
        <div class="viz-charts">
          <div class="viz-chart-container">
            <h3 class="viz-chart-title">情感分布</h3>
            <div ref="pieEl" class="viz-chart"></div>
          </div>
          <div class="viz-chart-container">
            <h3 class="viz-chart-title">各情绪类别平均得分</h3>
            <div ref="barEl" class="viz-chart"></div>
          </div>
        </div>
      </div>
      <!-- 卡片2：传播节奏与爆点分析 -->
      <div class="viz-panel">
        <h3 class="viz-panel-title">传播节奏与爆点分析</h3>
        <div class="viz-charts">
          <div class="viz-chart-container">
            <h4 class="viz-chart-subtitle">评论</h4>
            <div ref="commentsTimelineEl" class="viz-chart-small"></div>
          </div>
          <div class="viz-chart-container">
            <h4 class="viz-chart-subtitle">弹幕</h4>
            <div ref="danmakuTimelineEl" class="viz-chart-small"></div>
          </div>
        </div>
      </div>
      <!-- 卡片3：关键词差异分析 -->
      <div class="viz-panel">
        <h3 class="viz-panel-title">关键词差异分析</h3>
        <!-- 使用 Element Plus 提供的 tooltip 对话框为话题差异度提供解释 -->
        <el-tooltip
          v-if="diffScore !== null"
          class="viz-diff-score"
          effect="light"
          placement="right-start"
        >
          <template #content>
            <div style="max-width: 250px; line-height: 1.4">
              话题差异度反映评论与弹幕讨论内容之间的差异程度。<br/>
              值越大，表示两者的话题差别越明显；值越小，表示讨论内容越相近。
            </div>
          </template>
          <span>
            话题差异度：{{ diffScore.toFixed(2) }}
            <i style="margin-left: 4px; font-style: normal; font-size: 14px;">ℹ️</i>
          </span>
        </el-tooltip>
        <div class="viz-charts">
          <div class="viz-chart-container">
            <h4 class="viz-chart-subtitle">评论高频词</h4>
            <div ref="commentsWordsEl" class="viz-chart-small"></div>
          </div>
          <div class="viz-chart-container">
            <h4 class="viz-chart-subtitle">弹幕高频词</h4>
            <div ref="danmakuWordsEl" class="viz-chart-small"></div>
          </div>
        </div>
      </div>

      <!-- 卡片4：话题 / 观点聚类分析 -->
      <div class="viz-panel" v-if="hasClusters">
        <h3 class="viz-panel-title">话题 / 观点聚类分析</h3>
        <div class="viz-charts">
          <!-- 评论聚类 -->
          <div class="viz-chart-container">
            <h4 class="viz-chart-subtitle">评论话题分布</h4>
            <div ref="commentsClusterEl" class="viz-chart-small"></div>
            <ul class="viz-cluster-list" v-if="clustersComp?.comments?.top_words">
              <li v-for="(words, cid) in clustersComp.comments.top_words" :key="cid">
                话题 {{ cid }}：{{ words.slice(0, 5).join('、') }}
              </li>
            </ul>
          </div>
          <!-- 弹幕聚类 -->
          <div class="viz-chart-container">
            <h4 class="viz-chart-subtitle">弹幕话题分布</h4>
            <div ref="danmakuClusterEl" class="viz-chart-small"></div>
            <ul class="viz-cluster-list" v-if="clustersComp?.danmaku?.top_words">
              <li v-for="(words, cid) in clustersComp.danmaku.top_words" :key="cid">
                话题 {{ cid }}：{{ words.slice(0, 5).join('、') }}
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
import * as echarts from "echarts";
// 引入 Element Plus 的 Tooltip 组件（若未全局引入，可提供类型提示）
import { ElTooltip } from "element-plus";
import { onMounted, onBeforeUnmount, watch, ref, computed } from "vue";

// 情感标签映射：将模型的英文标签转换为中文，便于用户理解
const sentimentLabels = {
  negative: "负面",
  neutral: "中性",
  positive: "正面",
};

const props = defineProps({
  counts: { type: Object, default: () => ({}) },
  avgWeighted: { type: Object, default: () => ({}) },
  timeline: { type: Object, default: () => ({}) },
  burstPoints: { type: Object, default: () => ({}) },
  anomalies: { type: Object, default: () => ({}) },
  topWords: { type: Object, default: () => ({}) },
  differenceScore: { type: Number, default: null }
  ,
  clusters: { type: Object, default: () => ({}) }
});

const pieEl = ref(null);
const barEl = ref(null);

// 新增：传播节奏图表元素引用
const commentsTimelineEl = ref(null);
const danmakuTimelineEl = ref(null);

// 新增：高频词图表元素引用
const commentsWordsEl = ref(null);
const danmakuWordsEl = ref(null);

let pieChart = null;
let barChart = null;

let commentsTimelineChart = null;
let danmakuTimelineChart = null;

let commentsWordsChart = null;
let danmakuWordsChart = null;

// 聚类图表
const commentsClusterEl = ref(null);
const danmakuClusterEl = ref(null);
let commentsClusterChart = null;
let danmakuClusterChart = null;

// 计算属性：聚类数据，便于模板访问和自动 unref
const clustersComp = computed(() => props.clusters);

// 判断是否存在聚类结果，以决定是否渲染卡片
const hasClusters = computed(() => {
  const c = props.clusters || {};
  const cc = c.comments?.counts || [];
  const dc = c.danmaku?.counts || [];
  return (cc && cc.length > 0) || (dc && dc.length > 0);
});

// 差异度得分作为计算属性，便于模板访问
const diffScore = computed(() => props.differenceScore);

function toPieData(counts) {
  return Object.entries(counts || {}).map(([k, v]) => {
    const nameZh = sentimentLabels[k] || k;
    return { name: nameZh, value: v };
  });
}
function toBarData(avgWeighted) {
  const entries = Object.entries(avgWeighted || {});
  return {
    x: entries.map(([k]) => sentimentLabels[k] || k),
    y: entries.map(([, v]) => Number(v))
  };
}

function render() {
  if (!pieChart || !barChart) return;

  const pieData = toPieData(props.counts);
  pieChart.setOption({
    tooltip: { trigger: "item" },
    series: [
      {
        type: "pie",
        radius: ["35%", "70%"],
        avoidLabelOverlap: true,
        itemStyle: { borderRadius: 10, borderColor: "#fff", borderWidth: 2 },
        label: { show: true, formatter: "{b}: {c}" },
        emphasis: {
          scale: true,
          scaleSize: 8 // “果冻感”靠 hover 放大
        },
        data: pieData
      }
    ],
    animationDuration: 600,
    animationEasing: "elasticOut"
  });

  const bar = toBarData(props.avgWeighted);
  barChart.setOption({
    tooltip: { trigger: "axis" },
    xAxis: { type: "category", data: bar.x },
    yAxis: { type: "value", name: "平均得分" },
    series: [
      {
        type: "bar",
        data: bar.y,
        // 调整柱状图宽度和间隔，避免过宽导致三条柱子无法并排
        barWidth: '30%',
        barCategoryGap: '10%',
        barMaxWidth: 50,
        emphasis: { scale: true }
      }
    ],
    animationDuration: 650,
    animationEasing: "elasticOut"
  });

  // 调整尺寸以适配父容器（初次渲染后 ECharts 会读取 0 宽度，需显式 resize）
  pieChart.resize();
  barChart.resize();

  // ===== 传播节奏与爆点图 =====
  // Helper to build bar data with highlight for bursts/anomalies
  function buildTimelineOption(key) {
    const tl = props.timeline?.[key] || { bins: [], counts: [] };
    const bursts = props.burstPoints?.[key] || [];
    const anns = props.anomalies?.[key] || [];
    const counts = tl.counts || [];
    const bins = tl.bins || [];
    // 根据时间边界生成可读的时间段标签。
    const categories = [];
    for (let i = 0; i < bins.length - 1; i++) {
      const start = bins[i];
      const end = bins[i + 1];
      let label;
      if (key === 'danmaku') {
        // 弹幕使用 video_time（秒），转换为 mm:ss 区间
        const toTime = (s) => {
          const sec = Math.floor(s);
          const m = Math.floor(sec / 60);
          const s2 = (sec % 60).toString().padStart(2, '0');
          return `${m}:${s2}`;
        };
        label = `${toTime(start)}-${toTime(end)}`;
      } else {
        // 评论并无严格的时间轴，这里仍然使用段号表示，避免误解
        label = `段${i}`;
      }
      categories.push(label);
    }
    const data = counts.map((c, i) => {
      let color = '#5470c6'; // default blue
      if (anns.includes(i)) {
        color = '#e74c3c'; // red for anomalies
      } else if (bursts.includes(i)) {
        color = '#f39c12'; // orange for bursts
      }
      return { value: c, itemStyle: { color } };
    });
    // 计算横轴标签间隔，避免显示过于密集
    const catLength = categories.length;
    // 默认 interval=0 表示全部显示。若分类数量较多，则控制最多显示约五个标签
    let intervalValue = 0;
    if (catLength > 10) {
      // 显示约五个标签：ceil(长度/5)
      intervalValue = Math.ceil(catLength / 5);
    }
    return {
      tooltip: { trigger: 'axis' },
      xAxis: {
        type: 'category',
        data: categories,
        name: key === 'danmaku' ? '视频时间段' : '评论段',
        axisLabel: {
          interval: intervalValue,
          // 对弹幕时间段标签旋转一定角度便于显示
          rotate: key === 'danmaku' ? 30 : 0
        }
      },
      yAxis: { type: 'value', name: '消息数' },
      series: [
        {
          type: 'bar',
          data,
        },
      ],
      animationDuration: 600,
      animationEasing: 'cubicOut',
    };
  }
  if (commentsTimelineChart && danmakuTimelineChart) {
    // comments timeline
    const optC = buildTimelineOption('comments');
    commentsTimelineChart.setOption(optC);
    // danmaku timeline
    const optD = buildTimelineOption('danmaku');
    danmakuTimelineChart.setOption(optD);
    commentsTimelineChart.resize();
    danmakuTimelineChart.resize();
  }

  // ===== 高频词图 =====
  function buildWordsOption(key) {
    const tw = props.topWords?.[key] || [];
    const xData = tw.map(([w]) => w);
    const yData = tw.map(([, c]) => c);
    return {
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: xData, axisLabel: { interval: 0, rotate: 30 } },
      yAxis: { type: 'value', name: '词频' },
      series: [
        {
          type: 'bar',
          data: yData,
        },
      ],
      animationDuration: 600,
      animationEasing: 'cubicOut',
    };
  }
  if (commentsWordsChart && danmakuWordsChart) {
    const optCw = buildWordsOption('comments');
    commentsWordsChart.setOption(optCw);
    const optDw = buildWordsOption('danmaku');
    danmakuWordsChart.setOption(optDw);
    commentsWordsChart.resize();
    danmakuWordsChart.resize();
  }

  // ===== 聚类图 =====
  function buildClusterOption(key) {
    const clusters = props.clusters?.[key] || { counts: [] };
    const counts = clusters.counts || [];
    // 使用中文名称表示簇，例如“簇0”、“簇1”
    // 使用更通俗的“话题”代替“簇”
    const categories = counts.map((_, i) => `话题${i}`);
    return {
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'category', data: categories },
      yAxis: { type: 'value', name: '数量' },
      series: [
        {
          type: 'bar',
          data: counts,
        },
      ],
      animationDuration: 600,
      animationEasing: 'cubicOut',
    };
  }
  if (commentsClusterChart && danmakuClusterChart) {
    const optCc = buildClusterOption('comments');
    commentsClusterChart.setOption(optCc);
    const optDc = buildClusterOption('danmaku');
    danmakuClusterChart.setOption(optDc);
    commentsClusterChart.resize();
    danmakuClusterChart.resize();
  }
}

function resize() {
  pieChart?.resize();
  barChart?.resize();
}

onMounted(() => {
  pieChart = echarts.init(pieEl.value);
  barChart = echarts.init(barEl.value);
  // 新增：初始化传播节奏和词频图表实例
  commentsTimelineChart = echarts.init(commentsTimelineEl.value);
  danmakuTimelineChart = echarts.init(danmakuTimelineEl.value);
  commentsWordsChart = echarts.init(commentsWordsEl.value);
  danmakuWordsChart = echarts.init(danmakuWordsEl.value);

  // 初始化聚类图
  commentsClusterChart = echarts.init(commentsClusterEl.value);
  danmakuClusterChart = echarts.init(danmakuClusterEl.value);
  render();
  // 初次渲染后强制调整尺寸，避免容器宽度为 0 导致图形被压缩
  resize();
  window.addEventListener("resize", resize);
});

onBeforeUnmount(() => {
  window.removeEventListener("resize", resize);
  pieChart?.dispose();
  barChart?.dispose();
  commentsTimelineChart?.dispose();
  danmakuTimelineChart?.dispose();
  commentsWordsChart?.dispose();
  danmakuWordsChart?.dispose();

  commentsClusterChart?.dispose();
  danmakuClusterChart?.dispose();
});

watch(
  () => [props.counts, props.avgWeighted, props.timeline, props.burstPoints, props.anomalies, props.topWords, props.clusters],
  () => render(),
  { deep: true }
);
</script>

<style scoped>
/* 容器自适应，自动换行以避免图形被压缩 */
.viz { width: 100%; }

/* 自定义类名前缀，以避免与全局样式冲突 */
.viz-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 18px;
}
.viz-panel {
  border-radius: 18px;
  padding: 18px;
  background: #fff;
  box-shadow: 0 10px 30px rgba(0,0,0,.06);
  /* 占满父容器宽度，并取消对宽度的限制 */
  flex: 1 1 100%;
  width: 100%;
}
.viz-charts {
  display: flex;
  flex-wrap: wrap;
  gap: 18px;
}
.viz-chart-container {
  /* 每个图容器占一半宽度，减去间隙的一半以便对齐 */
  flex: 1 1 calc(50% - 9px);
  padding: 12px;
  box-sizing: border-box;
}
.viz-chart {
  width: 100%;
  height: 300px;
}

/* 小图尺寸，用于传播节奏和词频条形图 */
.viz-chart-small {
  width: 100%;
  height: 260px;
}

/* 面板标题和子标题样式 */
.viz-panel-title {
  font-size: 20px;
  font-weight: 600;
  margin: 0 0 12px 0;
  text-align: left;
}
.viz-chart-subtitle {
  font-size: 16px;
  margin: 6px 0;
  text-align: center;
}

/* 差异度得分样式 */
.viz-diff-score {
  font-size: 14px;
  margin-bottom: 8px;
  color: #555;
}

/* 聚类列表样式 */
.viz-cluster-list {
  margin-top: 8px;
  padding-left: 0;
  list-style: none;
  font-size: 14px;
  color: #333;
}
.viz-cluster-list li {
  line-height: 1.4;
}

/* 标题样式：统一字体大小和间距 */
.viz-chart-title {
  text-align: center;
  margin: 6px 0 14px;
  font-size: 18px;
}
</style>
