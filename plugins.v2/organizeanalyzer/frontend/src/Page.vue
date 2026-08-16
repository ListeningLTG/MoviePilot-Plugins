<template>
  <v-card flat class="organize-analyzer-modal-page pa-4" min-width="320">
    <!-- 头部信息栏 Header -->
    <div class="d-flex align-center justify-space-between flex-wrap gap-2 mb-4">
      <div class="d-flex align-center">
        <v-avatar color="primary" variant="tonal" size="44" class="mr-3">
          <v-icon icon="mdi-file-find-outline" size="28"></v-icon>
        </v-avatar>
        <div>
          <div class="d-flex align-center gap-2">
            <span class="text-h6 font-weight-bold">媒体整理异常分析</span>
            <v-chip
              size="small"
              :color="currentEnabled ? 'success' : 'grey'"
              variant="flat"
              class="font-weight-medium"
            >
              {{ currentEnabled ? '运行中' : '已禁用' }}
            </v-chip>
          </div>
          <div class="text-caption text-medium-emphasis mt-1 d-flex align-center flex-wrap">
            <v-icon icon="mdi-clock-outline" size="14" class="mr-1"></v-icon>
            上次分析: {{ stats.last_run_time || '尚未运行' }}
          </div>
        </div>
      </div>

      <!-- 右上角快捷操作 -->
      <div class="d-flex align-center gap-2">
        <v-btn
          color="primary"
          variant="tonal"
          size="small"
          :loading="analyzing"
          prepend-icon="mdi-play"
          @click="runQuickAnalyze"
        >
          立即增量分析
        </v-btn>
        <v-btn
          color="secondary"
          variant="tonal"
          size="small"
          prepend-icon="mdi-cog-outline"
          @click="goToConfig"
        >
          前往设置
        </v-btn>
      </div>
    </div>

    <!-- 加载中骨架屏 -->
    <div v-if="loading" class="py-6 text-center">
      <v-progress-circular indeterminate color="primary" size="36"></v-progress-circular>
      <div class="text-caption text-medium-emphasis mt-2">正在获取分析数据与生效配置...</div>
    </div>

    <div v-else>
      <!-- 核心统计看板 Metrics Banner -->
      <v-row dense class="mb-4">
        <!-- 未处理异常总数 -->
        <v-col cols="12" sm="6">
          <v-card
            variant="tonal"
            :color="summary.total > 0 ? 'error' : 'success'"
            class="pa-4 d-flex align-center justify-space-between rounded-lg"
          >
            <div>
              <div class="text-caption font-weight-medium text-uppercase">未处理异常</div>
              <div class="text-h4 font-weight-bold mt-1">{{ summary.total || 0 }}</div>
              <div class="text-caption mt-1 opacity-80">
                {{ summary.total > 0 ? '存在需要关注的整理冲突或未中文化' : '媒体库整理记录状态良好' }}
              </div>
            </div>
            <v-icon
              :icon="summary.total > 0 ? 'mdi-alert-circle-outline' : 'mdi-check-circle-outline'"
              size="48"
              class="opacity-70"
            ></v-icon>
          </v-card>
        </v-col>

        <!-- 已忽略白名单总数 -->
        <v-col cols="12" sm="6">
          <v-card
            variant="tonal"
            color="primary"
            class="pa-4 d-flex align-center justify-space-between rounded-lg"
          >
            <div>
              <div class="text-caption font-weight-medium text-uppercase">已忽略标记</div>
              <div class="text-h4 font-weight-bold mt-1">{{ summary.ignored || 0 }}</div>
              <div class="text-caption mt-1 opacity-80">
                已人工标记排除的非异常条目
              </div>
            </div>
            <v-icon icon="mdi-eye-off-outline" size="48" class="opacity-70"></v-icon>
          </v-card>
        </v-col>
      </v-row>

      <!-- 7 项细分异常指标 Chips Row -->
      <v-card variant="outlined" class="pa-3 mb-4 rounded-lg">
        <div class="text-subtitle-2 font-weight-bold mb-2 d-flex align-center">
          <v-icon icon="mdi-chart-box-outline" size="18" class="mr-1 text-primary"></v-icon>
          最近分析异常细项分布
        </div>
        <div class="d-flex flex-wrap gap-2">
          <v-chip
            size="small"
            variant="tonal"
            color="info"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-translate" size="14"></v-icon>
            英文/数字未中文化: {{ summary.english_title || 0 }}
          </v-chip>

          <v-chip
            size="small"
            variant="tonal"
            color="warning"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-file-multiple-outline" size="14"></v-icon>
            多文件合并覆盖: {{ summary.merged_files || 0 }}
          </v-chip>

          <v-chip
            size="small"
            variant="tonal"
            color="purple"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-help-circle-outline" size="14"></v-icon>
            未识别/TMDB缺失: {{ summary.unidentified || 0 }}
          </v-chip>

          <v-chip
            size="small"
            variant="tonal"
            color="error"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-close-circle-outline" size="14"></v-icon>
            整理运行失败: {{ summary.failed_status || 0 }}
          </v-chip>

          <v-chip
            size="small"
            variant="tonal"
            color="deep-orange"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-numeric" size="14"></v-icon>
            重复季集冲突: {{ summary.duplicate_episode || 0 }}
          </v-chip>

          <v-chip
            size="small"
            variant="tonal"
            color="grey-darken-1"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-file-remove-outline" size="14"></v-icon>
            目标缺失/0字节: {{ summary.missing_dest || 0 }}
          </v-chip>

          <v-chip
            size="small"
            variant="tonal"
            color="teal"
            class="font-weight-medium"
          >
            <v-icon start icon="mdi-chart-line-variant" size="14"></v-icon>
            离群集数异常: {{ summary.invalid_episode || 0 }}
          </v-chip>
        </div>
      </v-card>

      <!-- 当前生效的设置项 Configuration Overview -->
      <v-card variant="outlined" class="pa-3 mb-4 rounded-lg">
        <div class="d-flex align-center justify-space-between mb-3">
          <div class="text-subtitle-2 font-weight-bold d-flex align-center">
            <v-icon icon="mdi-tune" size="18" class="mr-1 text-primary"></v-icon>
            当前生效设置项
          </div>
          <v-btn
            size="x-small"
            variant="text"
            color="primary"
            prepend-icon="mdi-square-edit-outline"
            @click="goToConfig"
          >
            修改设置
          </v-btn>
        </div>

        <!-- 全局参数 -->
        <v-row dense class="mb-2">
          <v-col cols="12" sm="4">
            <div class="text-caption text-medium-emphasis">定时任务巡检</div>
            <div class="text-body-2 font-weight-medium mt-1">
              <v-icon
                :icon="effectiveConfig.cron_enabled ? 'mdi-check-circle' : 'mdi-minus-circle'"
                :color="effectiveConfig.cron_enabled ? 'success' : 'grey'"
                size="16"
                class="mr-1"
              ></v-icon>
              {{ effectiveConfig.cron_enabled ? `开启 [${effectiveConfig.cron_mode === 'incremental' ? '增量' : '全量'}] (${effectiveConfig.cron || '0 3 * * *'})` : '未开启' }}
            </div>
          </v-col>

          <v-col cols="12" sm="4">
            <div class="text-caption text-medium-emphasis">系统消息通知</div>
            <div class="text-body-2 font-weight-medium mt-1">
              <v-icon
                :icon="effectiveConfig.notify ? 'mdi-bell-check' : 'mdi-bell-off'"
                :color="effectiveConfig.notify ? 'success' : 'grey'"
                size="16"
                class="mr-1"
              ></v-icon>
              {{ effectiveConfig.notify ? '分析完成后推送报告' : '未开启通知' }}
            </div>
          </v-col>

          <v-col cols="12" sm="4">
            <div class="text-caption text-medium-emphasis">忽略路径白名单</div>
            <div class="text-body-2 font-weight-medium mt-1 text-truncate" :title="effectiveConfig.ignore_paths || '未设置'">
              <v-icon icon="mdi-filter-outline" color="primary" size="16" class="mr-1"></v-icon>
              {{ effectiveConfig.ignore_paths || '未设置' }}
            </div>
          </v-col>
        </v-row>

        <v-divider class="my-2"></v-divider>

        <!-- 检测规则生效状态 -->
        <div class="text-caption text-medium-emphasis mb-2">异常检测规则生效状态：</div>
        <div class="d-flex flex-wrap gap-2">
          <v-chip
            size="small"
            :color="effectiveConfig.detect_english_title !== false ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_english_title !== false ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_english_title !== false ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            英文/纯数字未中文化检测
          </v-chip>

          <v-chip
            size="small"
            :color="effectiveConfig.detect_merged_files !== false ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_merged_files !== false ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_merged_files !== false ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            多文件归并 (≥ {{ effectiveConfig.min_merged_files || 2 }})
          </v-chip>

          <v-chip
            size="small"
            :color="effectiveConfig.detect_unidentified !== false ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_unidentified !== false ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_unidentified !== false ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            未识别 / TMDB缺失
          </v-chip>

          <v-chip
            size="small"
            :color="effectiveConfig.detect_failed_status !== false ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_failed_status !== false ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_failed_status !== false ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            整理运行失败记录
          </v-chip>

          <v-chip
            size="small"
            :color="effectiveConfig.detect_duplicate_episode !== false ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_duplicate_episode !== false ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_duplicate_episode !== false ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            重复季集冲突
          </v-chip>

          <v-chip
            size="small"
            :color="effectiveConfig.detect_missing_dest ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_missing_dest ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_missing_dest ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            目标物理文件缺失/0字节
          </v-chip>

          <v-chip
            size="small"
            :color="effectiveConfig.detect_invalid_episode ? 'success' : 'grey'"
            :variant="effectiveConfig.detect_invalid_episode ? 'tonal' : 'outlined'"
          >
            <v-icon start :icon="effectiveConfig.detect_invalid_episode ? 'mdi-check' : 'mdi-close'" size="14"></v-icon>
            离群集数异常 (> {{ effectiveConfig.invalid_episode_threshold || 500 }})
          </v-chip>
        </div>
      </v-card>

      <!-- 独立大屏提示卡片 Navigation Guide -->
      <v-alert
        type="info"
        variant="tonal"
        icon="mdi-monitor-dashboard"
        class="mb-4 text-body-2 rounded-lg"
      >
        <div class="font-weight-medium">💡 想要查看全部异常文件明细或批量处理？</div>
        <div class="text-caption text-medium-emphasis mt-1">
          本插件已在 MoviePilot 左侧主导航栏<strong>【整理】</strong>分类下注册了<strong>【异常整理分析】</strong>独立大屏，支持分页筛选、复制路径、直达 TMDB 与一键忽略异常等完整操作。
        </div>
      </v-alert>
    </div>

    <!-- 底部操作按钮 Footer Actions -->
    <v-card-actions class="px-0 pb-0 pt-2 d-flex justify-end gap-2">
      <v-btn
        variant="outlined"
        color="secondary"
        prepend-icon="mdi-cog"
        @click="goToConfig"
      >
        前往设置
      </v-btn>
      <v-btn
        variant="elevated"
        color="primary"
        prepend-icon="mdi-check"
        @click="$emit('close')"
      >
        我知道了，关闭
      </v-btn>
    </v-card-actions>

    <!-- 提示消息 Snackbar -->
    <v-snackbar v-model="snackbar.show" :color="snackbar.color" timeout="3000" location="top">
      {{ snackbar.text }}
    </v-snackbar>
  </v-card>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'

const props = defineProps({
  initialConfig: Object,
  api: Object,
  pluginId: {
    type: String,
    default: 'OrganizeAnalyzer'
  }
})

const emit = defineEmits(['action', 'switch', 'close'])

const loading = ref(true)
const analyzing = ref(false)
const stats = ref({})
const fetchedConfig = ref({})
const snackbar = ref({ show: false, text: '', color: 'success' })

// 合并配置，优先使用接口实时返回的配置，以 props.initialConfig 为底
const effectiveConfig = computed(() => {
  const cfg = {
    ...(props.initialConfig || {}),
    ...(fetchedConfig.value || {})
  }
  if (cfg.min_merged_files === undefined) cfg.min_merged_files = 2
  if (cfg.cron_mode === undefined) cfg.cron_mode = 'incremental'
  if (cfg.cron === undefined) cfg.cron = '0 3 * * *'
  if (cfg.invalid_episode_threshold === undefined) cfg.invalid_episode_threshold = 500
  return cfg
})

const currentEnabled = computed(() => {
  if (stats.value.enabled !== undefined) return Boolean(stats.value.enabled)
  return Boolean(effectiveConfig.value.enabled)
})

const summary = computed(() => stats.value.summary || {})

// 获取数据统计和配置
const fetchStats = async () => {
  loading.value = true
  try {
    const pluginKey = props.pluginId || 'OrganizeAnalyzer'
    let res = null
    if (props.api && props.api.get) {
      res = await props.api.get(`plugin/${pluginKey}/stats`)
    } else {
      // 备用 fetch
      const resp = await fetch(`/api/v1/plugin/${pluginKey}/stats`)
      res = await resp.json()
    }

    const data = res?.data || res || {}
    stats.value = data
    if (data.config) {
      fetchedConfig.value = data.config
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] 获取统计数据失败:', err)
  } finally {
    loading.value = false
  }
}

// 快速发起增量分析
const runQuickAnalyze = async () => {
  analyzing.value = true
  try {
    const pluginKey = props.pluginId || 'OrganizeAnalyzer'
    if (props.api && props.api.post) {
      await props.api.post(`plugin/${pluginKey}/analyze?mode=incremental`)
    } else {
      await fetch(`/api/v1/plugin/${pluginKey}/analyze?mode=incremental`, { method: 'POST' })
    }
    snackbar.value = { show: true, text: '增量分析已完成！', color: 'success' }
    await fetchStats()
  } catch (err) {
    console.error('[OrganizeAnalyzer] 触发分析失败:', err)
    snackbar.value = { show: true, text: '触发分析失败，请检查插件状态', color: 'error' }
  } finally {
    analyzing.value = false
  }
}

// 跳转至设置 Tab
const goToConfig = () => {
  emit('switch', 'config')
  emit('action', { action: 'switch', target: 'config' })
}

onMounted(() => {
  fetchStats()
})
</script>

<style scoped>
.organize-analyzer-modal-page {
  max-width: 860px;
  margin: 0 auto;
}
.gap-2 {
  gap: 8px;
}
</style>
