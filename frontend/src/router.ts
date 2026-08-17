import { createRouter, createWebHistory } from 'vue-router'

export const routes = [
  { path: '/', name: 'overview', component: () => import('./views/OverviewView.vue'), meta: { title: '研究工作台' } },
  { path: '/market', name: 'market-review', component: () => import('./views/MarketReviewView.vue'), meta: { title: '市场全景' } },
  { path: '/data', name: 'data', component: () => import('./views/DataCenterView.vue'), meta: { title: '数据中心' } },
  { path: '/experiments', name: 'experiments', component: () => import('./views/ExperimentsView.vue'), meta: { title: '实验中心' } },
  { path: '/experiments/:experimentId', name: 'experiment-detail', component: () => import('./views/ExperimentDetailView.vue'), meta: { title: '实验详情' } },
  { path: '/factors', name: 'factors', component: () => import('./views/FactorsView.vue'), meta: { title: '因子分析' } },
  { path: '/tasks', name: 'tasks', component: () => import('./views/TasksView.vue'), meta: { title: '运行中心' } },
  { path: '/notebook', name: 'notebook', component: () => import('./views/NotebookView.vue'), meta: { title: 'Notebook', fullBleed: true } },
]

export default createRouter({ history: createWebHistory(), routes })
