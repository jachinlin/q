import { BarChart, HeatmapChart, LineChart, PieChart } from 'echarts/charts'
import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  VisualMapComponent,
} from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

use([
  CanvasRenderer,
  LineChart,
  BarChart,
  PieChart,
  HeatmapChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  VisualMapComponent,
])

export const axis = {
  axisLine: { lineStyle: { color: '#D8E2EE' } },
  axisTick: { show: false },
  axisLabel: { color: '#66778D', fontSize: 10 },
  splitLine: { lineStyle: { color: 'rgba(125,141,163,.18)' } },
}

export const tooltip = {
  trigger: 'axis',
  backgroundColor: '#FFFFFF',
  borderColor: '#D8E2EE',
  textStyle: { color: '#172033', fontSize: 11 },
}
