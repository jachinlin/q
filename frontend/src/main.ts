import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { createApp } from 'vue'

import App from './App.vue'
import router from './router'
import './styles.css'

const app = createApp(App)
app.use(router)
app.use(VueQueryPlugin, {
  queryClient: new QueryClient({
    defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } },
  }),
})
app.mount('#app')
