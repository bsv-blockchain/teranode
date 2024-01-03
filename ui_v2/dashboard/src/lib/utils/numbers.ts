export function clamp(value, min = -1, max = -1) {
  let val = value
  if (min !== -1) {
    val = Math.max(val, min)
  }
  if (max !== -1) {
    val = Math.min(val, max)
  }
  return val
}
