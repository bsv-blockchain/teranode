export const oneDayMillis = 1000 * 60 * 60 * 24

export const getData = (dataFrom: string, dateTo: string) => {
  const startTime = new Date(dataFrom).getTime()
  const endTime = new Date(dateTo).getTime()

  const startValue = 300000
  const dayMillis = 1000 * 60 * 60 * 24
  const result: any[] = []
  let currentTime = startTime
  while (currentTime <= endTime) {
    const delta = currentTime - startTime
    const daysPassed = delta / dayMillis
    const gainPerDay = 3000
    result.push({
      timestamp: new Date(currentTime).toISOString(),
      tx_count: startValue + daysPassed * gainPerDay + gainPerDay * 7 * Math.random(),
    })
    currentTime += dayMillis
  }
  return result
}
