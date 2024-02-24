import { formatDate, formatLargeNumberStr } from '$lib/utils/format'
import { timeSeriesTooltipFormatter } from '$internal/utils/graph'

// See:
// - https://apache.github.io/echarts-handbook/en/basics/import/
// - https://echarts.apache.org/handbook/en/basics/release-note/v5-upgrade-guide/
// import * as echarts from 'echarts'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DatasetComponent,
  TransformComponent,
  LegendComponent,
} from 'echarts/components'
import { LabelLayout, UniversalTransition } from 'echarts/features'
import { CanvasRenderer } from 'echarts/renderers'

import type {
  // The series option types are defined with the SeriesOption suffix
  LineSeriesOption,
} from 'echarts/charts'
import type {
  // The component option types are defined with the ComponentOption suffix
  TitleComponentOption,
  TooltipComponentOption,
  GridComponentOption,
  DatasetComponentOption,
} from 'echarts/components'
import type { ComposeOption } from 'echarts/core'

// Create an Option type with only the required components and charts via ComposeOption
export type ECOption = ComposeOption<
  | LineSeriesOption
  | TitleComponentOption
  | TooltipComponentOption
  | GridComponentOption
  | DatasetComponentOption
>

echarts.use([
  LineChart,
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DatasetComponent,
  TransformComponent,
  LegendComponent,
  LabelLayout,
  UniversalTransition,
  CanvasRenderer,
])

export const getGraphObj = (t, data, period, smooth = false) => {
  // graph data
  const graphData: any[] = []
  if (data) {
    data.forEach((row) => {
      graphData.push([row.timestamp * 1000, row.tx_count])
    })
  }
  // graph mappers
  const graphMappers: any[] = [
    {
      series: t('graph.series.date'),
      name: t('graph.series.date'),
      format: 'time',
    },
    {
      series: t('graph.series.tx_count'),
      name: t('graph.series.tx_count'),
      format: 'number',
      itemStyle: {
        color: '#1778FF',
      },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, -0.7636, 0, 1, [
          {
            offset: 0,
            color: '#1778FF',
          },
          {
            offset: 1,
            color: 'rgba(23, 120, 255, 0)',
          },
        ]),
      },
    },
  ]

  let startDate = new Date().getTime()
  switch (period) {
    case '2h':
      startDate -= 2 * 60 * 60 * 1000
      break
    case '6h':
      startDate -= 6 * 60 * 60 * 1000
      break
    case '12h':
      startDate -= 12 * 60 * 60 * 1000
      break
    case '24h':
      startDate -= 24 * 60 * 60 * 1000
      break
    case '1w':
      startDate -= 7 * 24 * 60 * 60 * 1000
      break
    case '1m':
      startDate -= 30 * 24 * 60 * 60 * 1000
      break
    case '3m':
      startDate -= 90 * 24 * 60 * 60 * 1000
      break
  }

  // graph options
  let graphOptions: ECOption | null = null
  if (graphData?.length) {
    const seriesNames = [t('graph.series.tx_count')]
    graphOptions = {
      grid: { top: 44, right: 25, bottom: 60, left: 60 },
      dataset: {
        source: graphData,
        dimensions: [t('graph.series.date')].concat(seriesNames),
      },
      xAxis: [
        {
          type: 'time',
          axisLine: { onZero: true },
          axisLabel: {
            formatter: (value) => ' ' + formatDate(parseInt(value.toString()), false, false),
            padding: [0, 5, 0, 5],
            hideOverlap: true,
          },
          alignTicks: true,
          min: startDate,
          max: new Date().getTime(),
        },
      ],
      yAxis: [
        {
          type: 'value',
          id: t('graph.series.tx_count'),
          min: 0,
          max: 'dataMax',
          axisLabel: {
            formatter: (value) => formatLargeNumberStr(value),
          },
          splitLine: {
            show: false,
          },
        },
      ],
      series: seriesNames.map((seriesName) => {
        const result = graphMappers.filter((mapper) => mapper.series === seriesName)
        const mapper = result && result.length > 0 ? result[0] : null
        return {
          name: seriesName,
          type: 'line',
          smooth,
          symbol: 'circle', // Add this line to set the symbol shape
          symbolSize: 10, // And this line to set the symbol size
          yAxisIndex: mapper?.yAxisIndex ? mapper.yAxisIndex : 0,
          itemStyle: mapper?.itemStyle ? mapper.itemStyle : null,
          areaStyle: mapper?.areaStyle ? mapper.areaStyle : null,
          encode: {
            x: t('graph.series.date'),
            y: seriesName,
          },
        }
      }),
      textStyle: {
        fontWeight: 400,
        fontSize: 14,
        lineHeight: 24,
        color: '#8F8D94',
      },
      tooltip: [
        {
          trigger: 'axis',
          formatter: timeSeriesTooltipFormatter.bind(null, graphMappers) as any,
          textStyle: {
            fontWeight: 500,
            color: '#282933',
          },
        },
      ],
      legend: {
        data: seriesNames,
        bottom: 0,
        textStyle: {
          fontWeight: 500,
          color: 'rgba(255, 255, 255, 0.66)',
        },
      },
    }
  }
  return {
    graphData,
    graphMappers,
    graphOptions,
  }
}
