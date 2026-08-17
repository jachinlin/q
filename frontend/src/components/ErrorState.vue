<script setup lang="ts">
defineProps<{ title?: string; message?: string }>()
</script>
<template>
  <div class="error-state">
    <span>!</span>
    <div><strong>{{ title ?? '数据暂时不可用' }}</strong><p>{{ message ?? '请检查本地服务状态后重试。' }}</p></div>
  </div>
</template>
