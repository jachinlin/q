<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ title?: string; message?: string; error?: unknown }>()
const errorCode = computed(() => {
  if (typeof props.error !== 'object' || props.error === null || !('code' in props.error)) return null
  const value = (props.error as { code?: unknown }).code
  return typeof value === 'string' ? value : null
})
const errorMessage = computed(() => {
  if (props.message) return props.message
  if (props.error instanceof Error && props.error.message) return props.error.message
  return props.error ? String(props.error) : '请检查本地服务状态后重试。'
})
const remediation = computed(() => {
  if (typeof props.error !== 'object' || props.error === null || !('remediation' in props.error)) return null
  const value = (props.error as { remediation?: unknown }).remediation
  return typeof value === 'string' && value ? value : null
})
</script>
<template>
  <div class="error-state">
    <span>!</span>
    <div><strong>{{ title ?? (errorCode ? `请求未通过 · ${errorCode}` : '数据暂时不可用') }}</strong><p>{{ errorMessage }}</p><small v-if="remediation">{{ remediation }}</small></div>
  </div>
</template>
