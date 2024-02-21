import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'
import * as api from '$internal/api'

export const messages = writable([])
export const miningNodes: any = writable({})
export const wsUrl: Writable<any> = writable('')
export const error: Writable<any> = writable(null)
export const sock: Writable<any> = writable(null)

const maxMessages = 100

export function connectToP2PServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'

    if (url.hostname.includes('ubsv.dev')) {
      url.port = url.protocol === 'ws:' ? '9906' : '9904'
    }

    if (url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
      url.protocol = 'ws:'
      url.host = 'localhost'
      url.port = '9906'
    }

    url.pathname = '/p2p-ws'

    wsUrl.set(url)
    error.set(null)

    let socket: any = new WebSocket(url)

    socket.onerror = (event: any) => {
      error.set(event)
      console.log('WebSocket Error:', event)
    }

    socket.onopen = () => {
      error.set(null)
      sock.set(socket)
      console.log(`p2pWS connection opened to ${url}`)
    }

    socket.onmessage = async (event: any) => {
      try {
        const data = await event.data
        const json: any = JSON.parse(data)

        json.receivedAt = new Date()

        let baseUrl = json.base_url
        if (!json.base_url.includes('localhost') && json.base_url.includes('http:')) {
          baseUrl = baseUrl.replace('http:', 'https:')
        }

        const miningNodeSet: any = get(miningNodes)
        if (json.type === 'mining_on') {
          miningNodeSet[baseUrl] = json
          miningNodes.set(miningNodeSet)
        } else if (baseUrl && !miningNodeSet[baseUrl]) {
          miningNodeSet[baseUrl] = {
            base_url: baseUrl,
          }
          const res: any = await api.getBestBlockHeaderFromServer({ baseUrl })
          if (res.ok && res.data) {
            miningNodeSet[baseUrl] = {
              ...res.data,
              tx_count: res.data.txCount,
              size_in_bytes: res.data.sizeInBytes,
              base_url: baseUrl,
            }
          }
          miningNodes.set(miningNodeSet)
        } else if (miningNodeSet[baseUrl]) {
          miningNodeSet[baseUrl].receivedAt = new Date()
          miningNodes.set(miningNodeSet)
        }
        //console.log('miningNodes', miningNodes)

        let m: any = get(messages)
        m = [json, ...m].slice(0, maxMessages)

        messages.set(m)
      } catch (error) {
        console.error('p2pWS: Error parsing WebSocket data:', error)
      }
    }

    socket.onclose = () => {
      error.set(new Error('closed'))
      console.log(`p2pWS connection closed by server (${url})`)
      socket = null
      sock.set(null)

      setTimeout(() => {
        connectToP2PServer()
      }, 5000) // Adjust the delay as necessary
    }
  }
}
