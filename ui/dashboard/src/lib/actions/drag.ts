export function drag(node) {
  let start: { x: any; y: any } | null = null

  const dispatchDelta = (nowPos) => {
    if (start) {
      node.dispatchEvent(
        new CustomEvent('dragmove', {
          detail: { x: nowPos.x - start.x, y: nowPos.y - start.y },
        }),
      )
    }
  }

  const handleMousemove = (event) => {
    dispatchDelta({ x: event.pageX, y: event.pageY })
  }

  const handleMousedown = (event) => {
    start = { x: event.pageX, y: event.pageY }
    node.dispatchEvent(new CustomEvent('dragstart'))
    document.addEventListener('mousemove', handleMousemove)
  }

  const handleMouseup = (event) => {
    dispatchDelta({ x: event.pageX, y: event.pageY })
    start = null
    document.removeEventListener('mousemove', handleMousemove)
    node.dispatchEvent(new CustomEvent('dragstop'))
  }

  node.addEventListener('mousedown', handleMousedown)
  document.addEventListener('mouseup', handleMouseup)

  return {
    destroy() {
      node.removeEventListener('mousedown', handleMousedown)
      document.removeEventListener('mouseup', handleMouseup)
      document.removeEventListener('mousemove', handleMousemove)
    },
  }
}
