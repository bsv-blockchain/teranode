export function humanTime(time: number) {
  let diff = new Date().getTime() - new Date(time).getTime()
  diff = diff / 1000
  const days = Math.floor(diff / 86400)
  const hours = Math.floor((diff % 86400) / 3600)
  const minutes = Math.floor(((diff % 86400) % 3600) / 60)
  const seconds = Math.floor(((diff % 86400) % 3600) % 60)

  let difference = ''

  if (days > 0) {
    difference += days
    difference += 'd'
    difference += hours
    difference += 'h'
    difference += minutes
    difference += 'm'
    difference += seconds
    difference += 's'
    return difference
  }

  if (hours > 0) {
    difference += hours
    difference += 'h'
    difference += minutes
    difference += 'm'
    difference += seconds
    difference += 's'
    return difference
  }

  if (minutes > 0) {
    difference += minutes
    difference += 'm'
    difference += seconds
    difference += 's'
    return difference
  }

  if (seconds > 0) {
    difference += seconds
    difference += 's'
    return difference
  }

  return '0s'
}

export function getHumanReadableTime(diff: number) {
  const days = Math.floor(diff / (1000 * 60 * 60 * 24))
  const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60))
  const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))
  const seconds = Math.floor((diff % (1000 * 60)) / 1000)

  if (days > 0) {
    return `${days}d${hours}h${minutes}m`
  } else if (hours > 0) {
    return `${hours}h${minutes}m`
  } else if (minutes > 0) {
    return `${minutes}m${seconds}s`
  } else {
    return `${seconds}s`
  }
}
