<script setup lang="ts">
defineProps<{
  label: string
  value: string | number
  hint?: string
  tone?: 'blue' | 'cyan' | 'red' | 'green'
}>()
</script>

<template>
  <article class="metric-card" :class="`tone-${tone ?? 'blue'}`">
    <div class="metric-top"><span>{{ label }}</span><i /></div>
    <strong class="metric-value tabular">{{ value }}</strong>
    <p>{{ hint ?? '—' }}</p>
  </article>
</template>
