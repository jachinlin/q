<script setup lang="ts">
import { useMutation, useQuery } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { parse, stringify } from 'yaml'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { ResearchTemplate, ResearchValidation } from '../types'

const router = useRouter()
const editorYaml = ref('')
const selectedTemplate = ref('')
const form = ref({ name: '', hypothesis: '', research_mode: '', strategy_id: '' })
const syncing = ref(false)

const templates = useQuery({
  queryKey: ['research-templates'],
  queryFn: () => api.get<{ items: ResearchTemplate[] }>('/api/v1/research/templates'),
})
const components = useQuery({
  queryKey: ['research-components'],
  queryFn: () => api.get<{ components: unknown[]; templates: unknown[] }>('/api/v1/research/components'),
})
const validate = useMutation({
  mutationFn: () => api.post<ResearchValidation>('/api/v1/research/validate', { config_yaml: editorYaml.value }),
  onSuccess: (result) => {
    editorYaml.value = result.normalized_yaml
    syncFormFromYaml()
    ElMessage.success(`配置有效：展开 ${result.variant_count} 个候选`)
  },
})
const submit = useMutation({
  mutationFn: () => api.post<{ family_id: string; execution_id: string; task_id: string; status: string }>('/api/v1/research/families', { config_yaml: editorYaml.value }),
  onSuccess: async (result) => {
    ElMessage.success('研究族已创建，候选展开任务已入队')
    await router.push(`/research/${result.family_id}`)
  },
})

const validationError = computed(() => {
  const error = validate.error.value ?? submit.error.value
  if (!error) return null
  return error instanceof DashboardApiError ? error : new Error(String(error))
})

watch(() => templates.data.value, (payload) => {
  if (!payload?.items.length || editorYaml.value) return
  applyTemplate(payload.items[0])
}, { immediate: true })

function applyTemplate(template: ResearchTemplate) {
  selectedTemplate.value = template.strategy_id
  editorYaml.value = template.yaml
  syncFormFromYaml()
  validate.reset()
}

function syncFormFromYaml() {
  try {
    const value = parse(editorYaml.value) as Record<string, unknown>
    syncing.value = true
    form.value = {
      name: String(value.name ?? ''),
      hypothesis: String(value.hypothesis ?? ''),
      research_mode: String(value.research_mode ?? ''),
      strategy_id: String(value.strategy_id ?? ''),
    }
  } catch {
    // 语法错误由后端严格校验展示，编辑过程中不覆盖表单。
  } finally {
    syncing.value = false
  }
}

function syncYamlFromForm() {
  if (syncing.value) return
  try {
    const value = parse(editorYaml.value) as Record<string, unknown>
    value.name = form.value.name
    value.hypothesis = form.value.hypothesis
    value.research_mode = form.value.research_mode
    value.strategy_id = form.value.strategy_id
    editorYaml.value = stringify(value, { lineWidth: 0 })
    validate.reset()
  } catch {
    ElMessage.warning('当前 YAML 语法不完整，修复后才能由表单同步')
  }
}

watch(editorYaml, syncFormFromYaml)
</script>

<template>
  <div class="page-stack research-composer">
    <section class="panel composer-header">
      <div><span class="eyebrow">COMPOSITION WORKBENCH</span><h2>研究编排器</h2><p>表单与 YAML 双向同步；规范化、能力校验和候选展开只认后端结果。</p></div>
      <div class="toolbar">
        <RouterLink to="/research"><el-button>返回</el-button></RouterLink>
        <el-button :loading="validate.isPending.value" @click="validate.mutate()">校验与预览</el-button>
        <el-button type="primary" :loading="submit.isPending.value" :disabled="!validate.data.value" @click="submit.mutate()">提交研究族</el-button>
      </div>
    </section>

    <section class="template-strip">
      <button v-for="template in templates.data.value?.items ?? []" :key="template.strategy_id" type="button" :class="{ active: selectedTemplate === template.strategy_id }" @click="applyTemplate(template)">
        <strong>{{ template.label }}</strong><small>{{ template.signal_kind }}</small>
      </button>
    </section>

    <ErrorState v-if="validationError" :error="validationError" />
    <section class="composer-grid">
      <div class="panel form-panel">
        <div class="panel-heading"><div><h2>研究定义</h2><p>修改字段会写回 YAML；协议和组件参数可在右侧完整编辑。</p></div></div>
        <el-form label-position="top">
          <el-form-item label="研究名称"><el-input v-model="form.name" @change="syncYamlFromForm" /></el-form-item>
          <el-form-item label="研究假设"><el-input v-model="form.hypothesis" type="textarea" :rows="3" @change="syncYamlFromForm" /></el-form-item>
          <el-form-item label="研究模式">
            <el-select v-model="form.research_mode" style="width:100%" @change="syncYamlFromForm">
              <el-option label="信号研究" value="SIGNAL_STUDY" /><el-option label="组合研究" value="PORTFOLIO_STUDY" /><el-option label="回测实验" value="BACKTEST_EXPERIMENT" />
            </el-select>
          </el-form-item>
          <el-form-item label="策略模板"><el-input v-model="form.strategy_id" disabled /></el-form-item>
        </el-form>
        <div class="schema-note"><strong>组件目录</strong><span>已加载 {{ components.data.value?.components.length ?? 0 }} 个不可变描述符</span></div>
      </div>
      <div class="panel yaml-panel">
        <div class="panel-heading"><div><h2>严格 YAML</h2><p>日期必须为 YYYY-MM-DD；搜索空间最多展开 256 个候选。</p></div><span class="hash">{{ validate.data.value?.config_hash.slice(0, 12) ?? 'UNVALIDATED' }}</span></div>
        <el-input v-model="editorYaml" type="textarea" :rows="34" spellcheck="false" class="yaml-editor" />
      </div>
      <aside class="panel preview-panel">
        <div class="panel-heading"><div><h2>候选预览</h2><p>字段路径稳定排序后做确定性笛卡尔积。</p></div></div>
        <template v-if="validate.data.value">
          <div class="preview-count"><strong>{{ validate.data.value.variant_count }}</strong><span>个候选</span></div>
          <dl><dt>信号类型</dt><dd>{{ validate.data.value.signal_kind }}</dd><dt>所需数据集</dt><dd>{{ validate.data.value.required_datasets.join(' · ') }}</dd></dl>
          <div class="variant-preview">
            <article v-for="item in validate.data.value.variants" :key="item.variant_id">
              <span class="hash">{{ item.variant_id.slice(0, 10) }}</span>
              <code>{{ JSON.stringify(item.parameters) }}</code>
            </article>
          </div>
        </template>
        <div v-else class="empty-state"><strong>等待后端校验</strong><small>将展示规范 YAML、数据需求和前 20 个候选</small></div>
      </aside>
    </section>
  </div>
</template>

<style scoped>
.composer-header { display:flex; align-items:center; justify-content:space-between; gap:20px; }
.composer-header h2 { margin:8px 0 5px; font-size:22px; }.composer-header p { margin:0; color:var(--dim); font-size:11px; }
.template-strip { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; }
.template-strip button { display:flex; align-items:center; justify-content:space-between; padding:15px; border:1px solid var(--border); border-radius:10px; color:var(--text); background:#fff; cursor:pointer; }
.template-strip button.active { border-color:var(--blue); box-shadow:0 0 0 2px rgba(37,99,235,.08); }.template-strip small { color:var(--dim); }
.composer-grid { display:grid; grid-template-columns:280px minmax(470px,1fr) minmax(280px,.6fr); gap:14px; align-items:start; }
.yaml-panel,.preview-panel,.form-panel { min-height:690px; }.yaml-editor :deep(textarea) { font:11px/1.55 ui-monospace,Consolas,monospace; }
.schema-note { display:flex; flex-direction:column; gap:5px; margin-top:20px; padding:12px; border-radius:8px; background:var(--surface-raised); font-size:11px; }.schema-note span { color:var(--dim); }
.preview-count { display:flex; align-items:baseline; gap:7px; }.preview-count strong { font-size:38px; }.preview-count span { color:var(--dim); }
.preview-panel dl { display:grid; grid-template-columns:74px 1fr; gap:8px; margin:20px 0; font-size:11px; }.preview-panel dt { color:var(--dim); }.preview-panel dd { margin:0; overflow-wrap:anywhere; }
.variant-preview { max-height:440px; overflow:auto; display:flex; flex-direction:column; gap:7px; }.variant-preview article { display:flex; flex-direction:column; gap:5px; padding:9px; border:1px solid var(--border); border-radius:7px; }.variant-preview code { overflow-wrap:anywhere; color:var(--muted); font-size:10px; }
@media(max-width:1500px){.composer-grid{grid-template-columns:260px 1fr}.preview-panel{grid-column:1/-1;min-height:auto}.variant-preview{display:grid;grid-template-columns:repeat(3,1fr)}}
</style>
