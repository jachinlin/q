import { createRouter, createWebHistory } from 'vue-router'

export const routes = [
  { path: '/', name: 'overview', component: () => import('./views/OverviewView.vue'), meta: { title: '研究工作台' } },
  { path: '/market', name: 'market-review', component: () => import('./views/MarketReviewView.vue'), meta: { title: '市场全景' } },
  { path: '/data', name: 'data', component: () => import('./views/DataCenterView.vue'), meta: { title: '数据中心' } },
  { path: '/strategy-studies', name: 'strategy-studies', component: () => import('./views/StrategyStudiesView.vue'), meta: { title: '策略研究' } },
  { path: '/strategy-studies/new', name: 'strategy-study-new', component: () => import('./views/StrategyStudyComposerView.vue'), meta: { title: '新建策略研究' } },
  { path: '/strategy-studies/:strategyStudyId', name: 'strategy-study-detail', component: () => import('./views/StrategyStudyDetailView.vue'), meta: { title: '策略研究详情' } },
  { path: '/factor-studies', name: 'factor-studies', component: () => import('./views/FactorStudiesView.vue'), meta: { title: '因子研究' } },
  { path: '/factor-studies/new', name: 'factor-study-new', component: () => import('./views/FactorStudyComposerView.vue'), meta: { title: '新建因子研究' } },
  { path: '/factor-studies/:factorStudyId', name: 'factor-study-detail', component: () => import('./views/FactorStudyDetailView.vue'), meta: { title: '因子研究详情' } },
  { path: '/tasks', name: 'tasks', component: () => import('./views/TasksView.vue'), meta: { title: '运行中心' } },
  { path: '/notebook', name: 'notebook', component: () => import('./views/NotebookView.vue'), meta: { title: 'Notebook', fullBleed: true } },
  { path: '/settings', name: 'settings', component: () => import('./views/SettingsView.vue'), meta: { title: '设置' } },
]

export default createRouter({ history: createWebHistory(), routes })
