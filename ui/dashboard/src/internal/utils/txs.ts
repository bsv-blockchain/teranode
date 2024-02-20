export const getTps = (transactionCount: number, diff: number) => {
  const timeDiff = diff / 1000 // The time diff between blocks (in seconds)

  if (timeDiff === 0) {
    return ''
  } else {
    const tps = transactionCount / timeDiff

    if (tps < 1) {
      return '<1'
    } else {
      return tps.toLocaleString(undefined, {
        maximumFractionDigits: 0,
        minimumFractionDigits: 0,
      })
    }
  }
}
