<template>
  <v-card flat class="text-center pa-10">
    <v-icon icon="mdi-open-in-new" size="64" color="primary" class="mb-4"></v-icon>
    <div class="text-h6 mb-2">插件数据已迁移</div>
    <div class="text-body-1 text-medium-emphasis mb-6">
      本插件的异常数据展示已经升级为全屏独立大屏仪表盘，无法在小窗口中完美展示。
      请关闭本窗口，然后点击 MoviePilot 主界面左侧导航栏中的 <strong>【异常整理分析】</strong> 菜单进行查看！
    </div>
    <v-btn color="primary" variant="elevated" @click="$emit('close')" prepend-icon="mdi-close">
      我知道了，关闭窗口
    </v-btn>
  </v-card>
</template>

<script setup>
const emit = defineEmits(['action', 'switch', 'close'])
</script>
