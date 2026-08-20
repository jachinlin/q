import { createRouter, createWebHistory } from 'vue-router'

export const routes = [
  { path: '/', name: 'overview', component: () => import('./views/OverviewView.vue'), meta: { title: '研究工作台' } },
  { path: '/market', name: 'market-review', component: () => import('./views/MarketReviewView.vue'), meta: { title: '市场全景' } },
  { path: '/data', name: 'data', component: () => import('./views/DataCenterView.vue'), meta: { title: '数据中心' } },
  { path: '/research', name: 'research', component: () => import('./views/ResearchCenterView.vue'), meta: { title: '研究中心' } },
  { path: '/research/new', name: 'research-new', component: () => import('./views/ResearchComposerView.vue'), meta: { title: '新建研究' } },
  { path: '/research/:familyId', name: 'research-detail', component: () => import('./views/ResearchDetailView.vue'), meta: { title: '研究详情' } },
  { path: '/tasks', name: 'tasks', component: () => import('./views/TasksView.vue'), meta: { title: '运行中心' } },
  { path: '/notebook', name: 'notebook', component: () => import('./views/NotebookView.vue'), meta: { title: 'Notebook', fullBleed: true } },
]

export default createRouter({ history: createWebHistory(), routes })
