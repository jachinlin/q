<script setup lang="ts">
import { statusType } from '../format'
defineProps<{ status: string }>()
</script>

<template>
  <el-tag size="small" effect="light" round :type="statusType(status)">{{ status || 'UNKNOWN' }}</el-tag>
</template>
