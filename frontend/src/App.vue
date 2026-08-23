<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'

const route = useRoute()
const collapsed = ref(false)
const title = computed(() => String(route.meta.title ?? '量化研究台'))

const nav = [
  ['/', '总览', '01'],
  ['/market', '市场全景', '02'],
  ['/data', '数据中心', '03'],
  ['/experiments', '实验中心', '04'],
  ['/tasks', '运行中心', '05'],
  ['/notebook', 'Notebook', '06'],
]
</script>

<template>
  <div class="app-shell" :class="{ 'is-collapsed': collapsed }">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">Q</div>
        <div v-if="!collapsed" class="brand-copy">
          <strong>QLAB</strong>
          <span>A-SHARE RESEARCH</span>
        </div>
      </div>
      <nav aria-label="主导航">
        <RouterLink v-for="item in nav" :key="item[0]" :to="item[0]" class="nav-item">
          <span class="nav-index">{{ item[2] }}</span>
          <span v-if="!collapsed">{{ item[1] }}</span>
        </RouterLink>
      </nav>
      <div class="sidebar-footer">
        <div class="local-state"><i /> <span v-if="!collapsed">本机安全连接</span></div>
        <button class="collapse-button" type="button" :aria-label="collapsed ? '展开导航' : '折叠导航'" @click="collapsed = !collapsed">
          {{ collapsed ? '›' : '‹' }}
        </button>
      </div>
    </aside>

    <main class="main-stage">
      <header class="topbar">
        <h1>{{ title }}</h1>
        <div class="topbar-meta">
          <span class="market-pill"><i /> 沪深研究环境</span>
          <time>{{ new Date().toLocaleDateString('zh-CN') }}</time>
        </div>
      </header>
      <div class="page-stage" :class="{ 'is-full-bleed': route.meta.fullBleed === true }">
        <RouterView v-slot="{ Component }">
          <Transition name="page" mode="out-in">
            <component :is="Component" />
          </Transition>
        </RouterView>
      </div>
    </main>
  </div>
</template>
