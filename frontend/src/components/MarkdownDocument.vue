<script setup lang="ts">
import DOMPurify from 'dompurify'
import { marked } from 'marked'
import { computed } from 'vue'

const props = defineProps<{ markdown: string }>()

const safeHtml = computed(() => {
  const rendered = marked.parse(props.markdown, { async: false })
  const sanitized = DOMPurify.sanitize(rendered, {
    FORBID_ATTR: ['style'],
    FORBID_TAGS: ['embed', 'iframe', 'object', 'script', 'style'],
  })
  const template = document.createElement('template')
  template.innerHTML = sanitized
  template.content.querySelectorAll('a').forEach((anchor) => {
    if (/^https?:\/\//i.test(anchor.getAttribute('href') ?? '')) {
      anchor.target = '_blank'
      anchor.rel = 'noopener noreferrer'
    }
  })
  return template.innerHTML
})
</script>

<template>
  <article class="markdown-document" v-html="safeHtml" />
</template>

<style scoped>
.markdown-document{color:var(--muted);font-size:13px;line-height:1.8}.markdown-document :deep(h1){display:none}.markdown-document :deep(h2){margin:30px 0 10px;padding-bottom:8px;border-bottom:1px solid var(--border);color:var(--text);font-size:17px}.markdown-document :deep(h3){margin:24px 0 8px;color:var(--text);font-size:14px}.markdown-document :deep(p){margin:9px 0}.markdown-document :deep(ul),.markdown-document :deep(ol){padding-left:22px}.markdown-document :deep(code){padding:2px 5px;border-radius:4px;background:#eef3f8;color:#355b84;font:12px ui-monospace,Consolas,monospace}.markdown-document :deep(pre){overflow:auto;padding:14px;border-radius:8px;background:#eef3f8}.markdown-document :deep(pre code){padding:0}.markdown-document :deep(table){width:100%;margin:14px 0;border-collapse:collapse}.markdown-document :deep(th),.markdown-document :deep(td){padding:9px 11px;border:1px solid var(--border);text-align:left}.markdown-document :deep(th){color:var(--text);background:var(--surface-raised)}.markdown-document :deep(a){color:var(--blue)}
</style>
