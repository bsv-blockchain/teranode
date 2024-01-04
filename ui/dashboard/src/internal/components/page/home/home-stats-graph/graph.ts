import * as echarts from 'echarts'

import { formatDate, addNumCommas } from '$lib/utils/format'
import { timeSeriesTooltipFormatter } from '$internal/utils/graph'

export const getGraphObj = (t, data, smooth = true) => {
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
  // graph options
  let graphOptions: any = null
  if (graphData?.length) {
    const seriesNames = [t('graph.series.tx_count')]
    graphOptions = {
      grid: { top: 44, right: 30, bottom: 60, left: 80 },
      dataset: {
        source: graphData,
        dimensions: [t('graph.series.date')].concat(seriesNames),
      },
      xAxis: {
        type: 'time',
        boundaryGap: false,
        axisLine: { onZero: true },
        axisLabel: {
          formatter: (value) => ' ' + formatDate(parseInt(value), false, false),
          padding: [0, 5, 0, 5],
          hideOverlap: true,
        },
        alignTicks: true,
        axisTick: {
          //   interval: 1,
        },
      },
      yAxis: [
        {
          type: 'value',
          id: t('graph.series.tx_count'),
          min: 0,
          max: 'dataMax',
          axisLabel: {
            formatter: (value) => addNumCommas(value),
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
          symbol: 'circle',    // Add this line to set the symbol shape
          symbolSize: 10,       // And this line to set the symbol size
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
        // fontFamily: 'Satoshi',
        fontWeight: 400,
        fontSize: 14,
        lineHeight: 24,
        color: '#8F8D94',
      },
      tooltip: {
        trigger: 'axis',
        formatter: timeSeriesTooltipFormatter.bind(null, graphMappers),
        textStyle: {
          fontWeight: 500,
          color: '#282933',
        },
      },
      legend: {
        data: seriesNames,
        bottom: 0,
        textStyle: {
          fontWeight: 500,
          color: 'rgba(255, 255, 255, 0.66)',
        },
      },
      // toolbox: {
      //   feature: {
      //     dataZoom: {
      //       yAxisIndex: 'none',
      //       filterMode: 'none',
      //       xAxisIndex: [0],
      //     },
      //     restore: { show: false },
      //     saveAsImage: {},
      //   },
      // },
    }
  }
  return {
    graphData,
    graphMappers,
    graphOptions,
  }
}
