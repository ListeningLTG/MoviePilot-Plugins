<template>
  <div class="pa-4 organize-analyzer-page">
    <!-- 顶部工具栏 Header -->
    <div class="d-flex align-center justify-space-between mb-4">
      <div>
        <h2 class="text-h5 font-weight-bold d-flex align-center">
          <v-icon icon="mdi-file-find-outline" class="mr-2" color="primary"></v-icon>
          媒体整理异常分析仪表盘
        </h2>
        <div class="text-caption text-medium-emphasis mt-1">
          上次运行时间: {{ stats.last_run_time || '尚未运行' }} | 
          定时分析状态: 
          <v-chip v-if="stats.cron_enabled" size="small" color="success" variant="tonal" class="ml-1">
            开启 [{{ stats.cron_mode === 'incremental' ? '增量' : '全量' }}] ({{ stats.cron }})
          </v-chip>
          <v-chip v-else size="small" color="grey" variant="tonal" class="ml-1">
            已禁用
          </v-chip>
        </div>
      </div>
      <div class="d-flex ga-2">
        <v-btn color="primary" :disabled="analyzing" @click="triggerAnalyze('incremental')">
          <template v-slot:prepend>
            <v-progress-circular v-if="analyzing" indeterminate size="20" width="2"></v-progress-circular>
            <v-icon v-else>mdi-play</v-icon>
          </template>
          立即增量分析
        </v-btn>
        <v-btn color="secondary" :disabled="analyzing" @click="triggerAnalyze('full')">
          <template v-slot:prepend>
            <v-progress-circular v-if="analyzing" indeterminate size="20" width="2"></v-progress-circular>
            <v-icon v-else>mdi-refresh</v-icon>
          </template>
          立即全量分析
        </v-btn>
        <v-btn color="info" variant="tonal" prepend-icon="mdi-clock-outline" @click="showCronDialog = true">定时配置</v-btn>
        <v-btn color="warning" variant="outlined" prepend-icon="mdi-delete-sweep" @click="clearIgnored">清空忽略</v-btn>
      </div>
    </div>

    <!-- 7 大统计卡片 Metrics Row -->
    <v-row class="mb-4">
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" :color="summary.total > 0 ? 'error' : 'success'" class="pa-3">
          <div class="text-subtitle-2 font-weight-medium">未处理异常总数</div>
          <div class="text-h4 font-weight-bold mt-1">{{ summary.total || 0 }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" color="warning" class="pa-3">
          <div class="text-subtitle-2">多文件覆盖冲突</div>
          <div class="text-h5 font-weight-bold mt-1">{{ summary.merged_files || 0 }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" color="info" class="pa-3">
          <div class="text-subtitle-2">英文标题未中文化</div>
          <div class="text-h5 font-weight-bold mt-1">{{ summary.english_title || 0 }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" color="purple" class="pa-3">
          <div class="text-subtitle-2">未识别 / TMDB缺失</div>
          <div class="text-h5 font-weight-bold mt-1">{{ summary.unidentified || 0 }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" color="error" class="pa-3">
          <div class="text-subtitle-2">整理运行失败</div>
          <div class="text-h5 font-weight-bold mt-1">{{ summary.failed_status || 0 }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" color="deep-orange" class="pa-3">
          <div class="text-subtitle-2">重复季集冲突</div>
          <div class="text-h5 font-weight-bold mt-1">{{ summary.duplicate_episode || 0 }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card variant="tonal" color="grey-darken-1" class="pa-3">
          <div class="text-subtitle-2">目标缺失/0字节</div>
          <div class="text-h5 font-weight-bold mt-1">{{ summary.missing_dest || 0 }}</div>
        </v-card>
      </v-col>
    </v-row>

    <!-- 筛选与搜索工具条 Filters Bar -->
    <v-card class="mb-4 pa-3">
      <v-row align="center" dense>
        <v-col cols="12" sm="4" md="3">
          <v-btn-toggle
            v-model="statusFilter"
            @update:modelValue="() => { pagination.page = 1; fetchExceptions(); }"
            mandatory
            color="primary"
            density="compact"
          >
            <v-btn value="active">未处理</v-btn>
            <v-btn value="ignored">已忽略</v-btn>
            <v-btn value="all">全部</v-btn>
          </v-btn-toggle>
        </v-col>
        <v-col cols="12" sm="4" md="4">
          <v-select
            v-model="typeFilter"
            @update:modelValue="() => { pagination.page = 1; fetchExceptions(); }"
            label="筛选异常类型"
            density="compact"
            hide-details
            :items="typeOptions"
          ></v-select>
        </v-col>
        <v-col cols="12" sm="4" md="5">
          <v-text-field
            v-model="keyword"
            @keyup.enter="fetchExceptions"
            @click:append-inner="fetchExceptions"
            label="搜索标题/路径关键字"
            density="compact"
            hide-details
            append-inner-icon="mdi-magnify"
          ></v-text-field>
        </v-col>
      </v-row>
    </v-card>

    <!-- 异常数据列表 Data Table Card -->
    <v-card>
      <v-table hover>
        <thead>
          <tr>
            <th class="text-left" style="width: 140px;">异常类型</th>
            <th class="text-left">标题 / 整理信息</th>
            <th class="text-left">源路径 src</th>
            <th class="text-left">目标路径 dest</th>
            <th class="text-left">异常原因明细</th>
            <th class="text-center" style="width: 110px;">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading">
            <td colspan="6" class="text-center text-medium-emphasis py-6">
              <v-progress-circular indeterminate size="24" width="2" class="mr-2"></v-progress-circular>
              加载中...
            </td>
          </tr>
          <tr v-else-if="exceptions.length === 0">
            <td colspan="6" class="text-center text-medium-emphasis py-6">
              暂无相关异常记录 🎉
            </td>
          </tr>
          <tr v-for="item in exceptions" :key="item.key">
            <td>
              <v-chip size="small" :color="getTypeColor(item.type)" variant="tonal">
                {{ item.type_name }}
              </v-chip>
            </td>
            <td>
              <div class="font-weight-medium">{{ item.title || '未知' }}</div>
              <div class="text-caption text-medium-emphasis">{{ item.date }}</div>
            </td>
            <td class="text-caption text-truncate" style="max-width: 200px;">{{ item.src || '-' }}</td>
            <td class="text-caption text-truncate" style="max-width: 200px;">{{ item.dest || '-' }}</td>
            <td class="text-body-2 text-warning">{{ item.detail || '-' }}</td>
            <td class="text-center">
              <v-btn
                size="x-small"
                variant="text"
                :color="item.status === 'ignored' ? 'primary' : 'grey'"
                @click="ignoreItem(item.key)"
              >
                {{ item.status === 'ignored' ? '取消忽略' : '忽略' }}
              </v-btn>
            </td>
          </tr>
        </tbody>
      </v-table>

      <!-- 分页栏 -->
      <div class="d-flex align-center justify-space-between px-4 py-3" v-if="pagination.total > 0">
        <div class="text-caption text-medium-emphasis">
          共 <strong>{{ pagination.total }}</strong> 条，当前第 {{ pagination.page }} / {{ pagination.total_pages }} 页
        </div>
        <div class="d-flex align-center ga-2">
          <v-select
            v-model="pagination.page_size"
            :items="[20, 50, 100, 200]"
            label="每页条数"
            density="compact"
            hide-details
            style="width: 110px;"
            @update:modelValue="() => { pagination.page = 1; fetchExceptions(); }"
          ></v-select>
          <v-pagination
            v-model="pagination.page"
            :length="pagination.total_pages"
            :total-visible="7"
            density="compact"
            @update:modelValue="fetchExceptions"
          ></v-pagination>
        </div>
      </div>
    </v-card>

    <!-- 定时分析配置弹窗 Cron Config Dialog -->
    <v-dialog v-model="showCronDialog" max-width="550px">
      <v-card>
        <v-card-title class="text-h6 pa-4">定时分析详细配置</v-card-title>
        <v-card-text class="pa-4">
          <v-switch
            v-model="cronForm.cron_enabled"
            label="开启后台定时自动分析"
            color="primary"
          ></v-switch>
          <v-select
            v-model="cronForm.cron_mode"
            label="定时分析执行模式"
            class="mt-2"
            :items="cronModeOptions"
          ></v-select>
          <v-text-field
            v-model="cronForm.cron"
            label="Cron 表达式"
            placeholder="0 3 * * *"
            hint="默认 0 3 * * * 代表每天凌晨 3:00 执行"
            persistent-hint
            class="mt-2"
          ></v-text-field>
          <v-switch
            v-model="cronForm.notify"
            label="分析完成后发送 Telegram/系统通知报告"
            color="primary"
            class="mt-2"
          ></v-switch>
        </v-card-text>
        <v-card-actions class="pa-4 pt-0">
          <v-spacer></v-spacer>
          <v-btn variant="text" @click="showCronDialog = false">取消</v-btn>
          <v-btn color="primary" variant="elevated" @click="saveCronConfig">保存生效</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';

const props = defineProps({
  api: { type: Object, required: true },
  pluginId: { type: String, default: 'OrganizeAnalyzer' },
  navKey: { type: String, default: 'main' }
});

const loading = ref(false);
const analyzing = ref(false);
const showCronDialog = ref(false);
const keyword = ref('');
const statusFilter = ref('active');
const typeFilter = ref('');
const pagination = ref({ page: 1, page_size: 50, total: 0, total_pages: 1 });

const stats = ref({
  summary: {
    total: 0,
    merged_files: 0,
    english_title: 0,
    unidentified: 0,
    failed_status: 0,
    duplicate_episode: 0,
    missing_dest: 0,
    invalid_episode: 0,
    ignored: 0
  },
  last_run_time: '尚未运行',
  cron_enabled: true,
  cron: '0 3 * * *',
  cron_mode: 'incremental',
  notify: false
});

const exceptions = ref([]);

const cronForm = ref({
  cron_enabled: true,
  cron: '0 3 * * *',
  cron_mode: 'incremental',
  notify: false
});

const typeOptions = [
  { title: '全部类型', value: '' },
  { title: '多文件合并覆盖', value: 'merged_files' },
  { title: '英文标题未中文化', value: 'english_title' },
  { title: '未识别/TMDB缺失', value: 'unidentified' },
  { title: '整理运行失败', value: 'failed_status' },
  { title: '重复季集冲突', value: 'duplicate_episode' },
  { title: '目标文件缺失/0字节', value: 'missing_dest' },
  { title: '离群/格式异常集数', value: 'invalid_episode' }
];

const cronModeOptions = [
  { title: '增量分析 (推荐，高速高效)', value: 'incremental' },
  { title: '全量分析 (重新完整扫描)', value: 'full' }
];

const summary = computed(() => stats.value.summary || {});

const fetchStats = async () => {
  try {
    const res = await props.api.get(`plugin/${props.pluginId}/stats`);
    if (res && res.data) {
      stats.value = res.data;
      cronForm.value = {
        cron_enabled: res.data.cron_enabled ?? true,
        cron: res.data.cron || '0 3 * * *',
        cron_mode: res.data.cron_mode || 'incremental',
        notify: res.data.notify ?? false
      };
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] Fetch stats failed', err);
  }
};

const fetchExceptions = async () => {
  loading.value = true;
  try {
    const res = await props.api.get(`plugin/${props.pluginId}/exceptions`, {
      params: {
        status: statusFilter.value,
        type_filter: typeFilter.value,
        keyword: keyword.value,
        page: pagination.value.page,
        page_size: pagination.value.page_size
      }
    });
    if (res && res.data !== undefined) {
      exceptions.value = res.data;
      pagination.value.total = res.total || 0;
      pagination.value.total_pages = res.total_pages || 1;
      pagination.value.page = res.page || 1;
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] Fetch exceptions failed', err);
  } finally {
    loading.value = false;
  }
};

const triggerAnalyze = async (mode = 'incremental') => {
  analyzing.value = true;
  try {
    await props.api.post(`plugin/${props.pluginId}/analyze?mode=${mode}`);
    await fetchStats();
    await fetchExceptions();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Trigger analyze failed', err);
  } finally {
    analyzing.value = false;
  }
};

const ignoreItem = async (key) => {
  try {
    await props.api.post(`plugin/${props.pluginId}/ignore?key=${encodeURIComponent(key)}`);
    await fetchStats();
    await fetchExceptions();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Ignore item failed', err);
  }
};

const clearIgnored = async () => {
  try {
    await props.api.post(`plugin/${props.pluginId}/clear_ignored`);
    await fetchStats();
    await fetchExceptions();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Clear ignored failed', err);
  }
};

const saveCronConfig = async () => {
  try {
    await props.api.post(`plugin/${props.pluginId}/save_cron_config`, cronForm.value);
    showCronDialog.value = false;
    await fetchStats();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Save cron config failed', err);
  }
};

const getTypeColor = (type) => {
  switch (type) {
    case 'merged_files': return 'warning';
    case 'english_title': return 'info';
    case 'unidentified': return 'purple';
    case 'failed_status': return 'error';
    case 'duplicate_episode': return 'deep-orange';
    case 'missing_dest': return 'grey-darken-1';
    default: return 'primary';
  }
};

onMounted(() => {
  fetchStats();
  fetchExceptions();
});
</script>
