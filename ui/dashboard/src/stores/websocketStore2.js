export function connectToWebSocket2(blobServerHTTPAddress) {
  const url = new URL(blobServerHTTPAddress)
  const wsUrl = `${url.protocol === 'http:' ? 'ws' : 'wss'}://${
    url.hostname
  }:8090/ws`

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`WebSocket2 connection opened to ${wsUrl}`)
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
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
