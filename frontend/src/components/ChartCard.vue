<script setup lang="ts">
defineProps<{ title: string; subtitle?: string; empty?: boolean }>()
</script>

<template>
  <section class="panel chart-card">
    <header class="panel-heading">
      <div><h2>{{ title }}</h2><p v-if="subtitle">{{ subtitle }}</p></div>
      <slot name="action" />
    </header>
    <div v-if="empty" class="empty-state"><span>暂无可用数据</span><small>完成并登记实验后将在此展示</small></div>
    <slot v-else />
  </section>
</template>
