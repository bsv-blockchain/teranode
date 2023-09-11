export function connectToWebSocket2() {
  const wsUrl = new URL('wss://bootstrap.ubsv.dev:8099/ws')

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`WebSocket2 connection opened to ${wsUrl}`)
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data.text()
      const json = JSON.parse(data)

      console.log('Websocket2', json)
    } catch (error) {
      console.error('Error2 parsing WebSocket data:', error)
    }
  }

  socket.onclose = () => {
    console.log(`WebSocket2 connection closed by server (${wsUrl})`)
    socket = null
  }
}
