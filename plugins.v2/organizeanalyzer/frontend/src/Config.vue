<template>
  <v-card flat>
    <v-card-text>
      <div class="text-subtitle-1 mb-4 font-weight-bold">
        全局设置
      </div>
      <v-row dense>
        <v-col cols="12" md="4">
          <v-switch v-model="config.enabled" label="启用插件" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="4">
          <v-switch v-model="config.notify" label="分析完成后发送系统通知" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="4">
          <v-switch v-model="config.cron_enabled" label="开启后台定时分析" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-text-field v-model="config.cron" label="定时 Cron 表达式" placeholder="0 3 * * *" density="compact" class="mt-2" hide-details></v-text-field>
        </v-col>
        <v-col cols="12" md="6">
          <v-select v-model="config.cron_mode" label="定时分析执行模式" :items="[{title: '增量分析 (推荐，高效速度快)', value: 'incremental'}, {title: '全量分析 (重新完整检索)', value: 'full'}]" density="compact" class="mt-2" hide-details></v-select>
        </v-col>
      </v-row>

      <v-divider class="my-4"></v-divider>
      
      <div class="text-subtitle-1 mb-4 font-weight-bold">
        异常检测规则开关及参数
      </div>
      <v-row dense>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_merged_files" label="检测多文件归并/覆盖同一目标" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-text-field v-model="config.min_merged_files" type="number" label="归并文件最小数量阈值" density="compact" hide-details></v-text-field>
        </v-col>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_english_title" label="检测英文未中文化标题" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_unidentified" label="检测未识别 / TMDB 缺失" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_failed_status" label="检测整理状态失败记录" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_duplicate_episode" label="检测重复季集冲突" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_missing_dest" label="检测目标物理文件缺失/0字节 (本地路径)" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-switch v-model="config.detect_invalid_episode" :label="`检测离群/格式异常集数 (>${config.invalid_episode_threshold || 500})`" color="primary" density="compact" hide-details></v-switch>
        </v-col>
        <v-col cols="12" md="6">
          <v-text-field v-model="config.invalid_episode_threshold" type="number" label="离群集数判断阈值" density="compact" hide-details></v-text-field>
        </v-col>
        <v-col cols="12">
          <v-textarea v-model="config.ignore_paths" label="忽略路径关键词白名单 (英文逗号分隔)" placeholder="/downloads/, /temp/" rows="2" density="compact" class="mt-2" hide-details></v-textarea>
        </v-col>
      </v-row>
    </v-card-text>

    <!-- 独立保存按钮，用于触发配置保存 -->
    <v-card-actions class="px-4 pb-4 pt-0">
      <v-spacer></v-spacer>
      <v-btn variant="text" @click="$emit('close')">取消</v-btn>
      <v-btn color="primary" variant="elevated" @click="save">保存设置</v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup>
import { ref, watch } from 'vue'

const props = defineProps({
  initialConfig: Object,
  api: Object
})

const emit = defineEmits(['save', 'close', 'switch'])

// 深度克隆一份配置供双向绑定
const config = ref(JSON.parse(JSON.stringify(props.initialConfig || {})))

// 确保默认值
if (config.value.min_merged_files === undefined) config.value.min_merged_files = 2
if (config.value.cron_mode === undefined) config.value.cron_mode = 'incremental'
if (config.value.cron === undefined) config.value.cron = '0 3 * * *'
if (config.value.invalid_episode_threshold === undefined) config.value.invalid_episode_threshold = 500

const save = () => {
  // 转换部分类型
  config.value.min_merged_files = parseInt(config.value.min_merged_files) || 2
  config.value.invalid_episode_threshold = parseInt(config.value.invalid_episode_threshold) || 500
  emit('save', config.value)
}
</script>
