import { spinCount } from '../stores/nav'
import { assetHTTPAddress } from '$internal/stores/nodeStore'

// Export the store directly for components that need to access it
export { assetHTTPAddress }

export enum ItemType {
  block = 'block',
  header = 'header',
  subtree = 'subtree',
  tx = 'tx',
  utxo = 'utxo',
  utxos = 'utxos',
  txmeta = 'txmeta',
}

function incSpinCount() {
  spinCount.update((n) => n + 1)
}

function decSpinCount() {
  spinCount.update((n) => n - 1)
}

interface ResponseData {
  data: any
}

function checkInitialResponse(response: Response): Promise<ResponseData> {
  return new Promise<ResponseData>((resolve, reject) => {
    if (response.ok) {
      let data: any = null

      const mimeType = response.headers.get('Content-Type') || 'text/plain'
      if (mimeType.toLowerCase().startsWith('application/json')) {
        response
          .json()
          .then((jsonData) => {
            data = jsonData
            resolve({ data })
          })
          .catch((e) => {
            data = null
            resolve({ data })
          })
      } else if (mimeType.toLowerCase().startsWith('application/octet-stream')) {
        response
          .blob()
          .then((blobData) => {
            data = blobData
            resolve({ data })
          })
          .catch((e) => {
            data = null
            resolve({ data })
          })
      } else {
        response
          .text()
          .then((textData) => {
            data = textData
            resolve({ data })
          })
          .catch((e) => {
            data = null
            resolve({ data })
          })
      }
    } else {
      response
        .json()
        .then((errorBody) => {
          reject({
            code: response.status,
            message: errorBody?.error || response.statusText || 'Unspecified error.',
          })
        })
        .catch((e) => {
          reject({
            code: response.status,
            message: response.statusText || 'Unspecified error.',
          })
        })
    }
  })
}

// Define API response type for better type safety
export type ApiResponse<T> = { ok: true; data: T } | { ok: false; error: any }

interface ApiOptions extends RequestInit {
  query?: Record<string, string>
}

function callApi<T>(url: string, options: ApiOptions = {}): Promise<ApiResponse<T>> {
  if (!options.method) {
    options.method = 'GET'
  }

  // Make sure we have headers
  if (!options.headers) {
    options.headers = {}
  }

  // Add the origin header for CORS if not already set
  if (!options.headers['Origin']) {
    options.headers['Origin'] = window.location.origin
  }

  incSpinCount()

  return fetch(url, options)
    .then(async (res) => {
      const { data } = await checkInitialResponse(res)
      return data
    })
    .then((data) => {
      return { ok: true, data } as ApiResponse<T>
    })
    .catch((error) => {
      console.log(error)
      return { ok: false, error } as ApiResponse<T>
    })
    .finally(decSpinCount)
}

function get<T>(url: string, options: ApiOptions = {}): Promise<ApiResponse<T>> {
  const { query, ...rest } = options
  const theUrl = query ? url + '?' + new URLSearchParams(query) : url
  return callApi<T>(theUrl, { ...rest, method: 'GET' })
}

// Uncomment and fix these functions if needed
// function post<T>(url: string, options: ApiOptions = {}): Promise<ApiResponse<T>> {
//   return callApi<T>(url, { ...options, method: 'POST' })
// }

// function put<T>(url: string, options: ApiOptions = {}): Promise<ApiResponse<T>> {
//   return callApi<T>(url, { ...options, method: 'PUT' })
// }

// function del<T>(url: string, options: ApiOptions = {}): Promise<ApiResponse<T>> {
//   return callApi<T>(url, { ...options, method: 'DELETE' })
// }

let baseUrl = ''
//const baseUrl = 'https://m1.scaling.teranode.network/api/v1'

assetHTTPAddress.subscribe((value) => {
  baseUrl = value
})

export const getItemApiUrl = (type: ItemType, hash: string) => {
  return `${baseUrl}/${type}/${hash}/json`
}

// api methods

export function getLastBlocks(data = {}): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/lastblocks?${new URLSearchParams(data)}`, {})
}

export function getItemData(data: { type: ItemType; hash: string }): Promise<ApiResponse<any>> {
  return get<any>(getItemApiUrl(data.type, data.hash), {})
}

export function getBlocks(data: { offset: number; limit: number }): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/blocks`, {
    query: { offset: String(data.offset), limit: String(data.limit) },
  })
}

export function getBestBlockHeaderFromServer(data: { baseUrl: string }): Promise<ApiResponse<any>> {
  return get<any>(`${data.baseUrl}/bestblockheader/json`, {})
}

export function getBlockSubtrees(data: {
  hash: string
  offset: number
  limit: number
}): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/block/${data.hash}/subtrees/json`, {
    query: { offset: String(data.offset), limit: String(data.limit) },
  })
}

export function getSubtreeTxs(data: {
  hash: string
  offset: number
  limit: number
}): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/subtree/${data.hash}/txs/json`, {
    query: { offset: String(data.offset), limit: String(data.limit) },
  })
}

export function getSubtreeNodes(data: {
  hash: string
  offset: number
  limit: number
}): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/subtree/${data.hash}/json`, {
    query: { offset: String(data.offset), limit: String(data.limit) },
  })
}

export function searchItem(data: { q: string }): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/search?${new URLSearchParams(data)}`, {})
}

export function getBlockStats(): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/blockstats`, { credentials: 'include' })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<any>
      }
      return handleApiError<any>(response.error, '/blockstats')
    })
    .catch((error) => handleApiError<any>(error, '/blockstats'))
}

export function getBlockForks(data: { hash: string; limit: number }): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/block/${data.hash}/forks`, { query: { limit: String(data.limit) } })
}

export function getBlockGraphData(data: { period: string }): Promise<ApiResponse<any>> {
  return get<any>(`${baseUrl}/blockgraphdata/${data.period}`, { credentials: 'include' })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<any>
      }
      return handleApiError<any>(response.error, `/blockgraphdata/${data.period}`)
    })
    .catch((error) => handleApiError<any>(error, `/blockgraphdata/${data.period}`))
}

// FSM API interfaces
export interface FSMState {
  state: string
  state_value: number
  message?: string
  lastTransition: number // Timestamp of the last state transition
}

export interface FSMEvent {
  name: string
  value: number
  description?: string // Description of the event
}

// Helper function to handle API errors
function handleApiError<T>(error: any, endpoint: string): ApiResponse<T> {
  console.error(`API error for ${endpoint}:`, error)

  // Check if it's a network error
  if (error instanceof TypeError && error.message.includes('Failed to fetch')) {
    return {
      ok: false,
      error: {
        message: 'Network error: Failed to connect to the server. Please check your connection.',
        originalError: error,
      },
    }
  }

  // Check if it's a CORS error
  if (error instanceof TypeError && error.message.includes('CORS')) {
    return {
      ok: false,
      error: {
        message: 'CORS error: The server is not configured to accept requests from this origin.',
        originalError: error,
      },
    }
  }

  // Handle HTTP errors
  if (error.status) {
    let message = `HTTP ${error.status}`

    if (error.status === 404) {
      // Check if this is a block operation
      if (endpoint.includes('/block/')) {
        message = 'Block hash not found: This hash is not a valid block in the blockchain'
      } else {
        message = `API endpoint not found: ${endpoint}`
      }
    } else if (error.status === 401 || error.status === 403) {
      message = 'Authentication error: You are not authorized to access this resource.'
    } else if (error.status === 500) {
      message = 'Server error: The server encountered an internal error.'
    } else if (error.status === 503) {
      message = 'Service unavailable: The blockchain service may not be running.'
    }

    return {
      ok: false,
      error: {
        message,
        status: error.status,
        originalError: error,
      },
    }
  }

  // Default error handling
  return {
    ok: false,
    error: {
      message: error.message || 'Unknown error occurred',
      originalError: error,
    },
  }
}

// Get the current FSM state
export function getFSMState(): Promise<ApiResponse<FSMState>> {
  return get<FSMState>(`${baseUrl}/fsm/state`, { credentials: 'include' })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, '/fsm/state')
    })
    .catch((error) => handleApiError<FSMState>(error, '/fsm/state'))
}

// Get available FSM events
export function getFSMEvents(): Promise<ApiResponse<FSMEvent[]>> {
  return get<{ events: string[] }>(`${baseUrl}/fsm/events`, { credentials: 'include' })
    .then((response) => {
      if (response.ok) {
        // Convert string array to FSMEvent objects with name and value
        const events: FSMEvent[] = response.data.events.map((eventName) => {
          // Assign specific IDs to known events for consistent ordering
          let value: number

          switch (eventName) {
            case 'RUN':
              value = 1
              break
            case 'STOP':
              value = 2
              break
            case 'CATCHUPBLOCKS':
              value = 3
              break
            case 'LEGACYSYNC':
              value = 4
              break
            default:
              // Try to extract a numeric ID from the event name if it exists
              const match = eventName.match(/[_-]?(\d+)$/)
              value = match ? parseInt(match[1], 10) : 999 // Default to high number for unknown events
          }

          return {
            name: eventName,
            value: value,
          }
        })

        console.log('Processed FSM events with assigned IDs:', events)
        return { ok: true, data: events } as ApiResponse<FSMEvent[]>
      }
      return handleApiError<FSMEvent[]>(response.error, '/fsm/events')
    })
    .catch((error) => handleApiError<FSMEvent[]>(error, '/fsm/events'))
}

// Get all possible FSM states
export function getFSMStates(): Promise<ApiResponse<FSMState[]>> {
  return get<{ states: FSMState[] }>(`${baseUrl}/fsm/states`, { credentials: 'include' })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data.states } as ApiResponse<FSMState[]>
      }
      return handleApiError<FSMState[]>(response.error, '/fsm/states')
    })
    .catch((error) => handleApiError<FSMState[]>(error, '/fsm/states'))
}

// Send a predefined FSM event
export function sendFSMEvent(event: string): Promise<ApiResponse<FSMState>> {
  return callApi<FSMState>(`${baseUrl}/fsm/${event.toLowerCase()}`, {
    method: 'POST',
    credentials: 'include',
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, `/fsm/${event.toLowerCase()}`)
    })
    .catch((error) => handleApiError<FSMState>(error, `/fsm/${event.toLowerCase()}`))
}

// Send a custom FSM event
export function sendCustomFSMEvent(eventName: string): Promise<ApiResponse<FSMState>> {
  // Get the current origin for CORS
  const origin = window.location.origin

  return callApi<FSMState>(`${baseUrl}/fsm/state`, {
    method: 'POST',
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      Origin: origin,
    },
    body: JSON.stringify({ event: eventName }),
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, '/fsm/state')
    })
    .catch((error) => handleApiError<FSMState>(error, '/fsm/state'))
}

// Invalidate a block by its hash
export function invalidateBlock(blockHash: string): Promise<ApiResponse<any>> {
  // Get the current origin for CORS
  const origin = window.location.origin

  return callApi<any>(`${baseUrl}/block/invalidate`, {
    method: 'POST',
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      Origin: origin,
    },
    body: JSON.stringify({ blockHash }),
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<any>
      }
      return handleApiError<any>(response.error, '/block/invalidate')
    })
    .catch((error) => handleApiError<any>(error, '/block/invalidate'))
}

// Revalidate a block by its hash
export function revalidateBlock(blockHash: string): Promise<ApiResponse<any>> {
  return new Promise<ApiResponse<any>>((resolve) => {
    incSpinCount()

    let baseUrl = ''
    assetHTTPAddress.subscribe((value) => {
      baseUrl = value
    })()

    fetch(`${baseUrl}/block/revalidate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ blockHash }),
      credentials: 'include',
    })
      .then((response) => checkInitialResponse(response))
      .then((data) => {
        resolve({ ok: true, data: data.data })
      })
      .catch((error) => {
        resolve(handleApiError(error, 'revalidateBlock'))
      })
      .finally(() => {
        decSpinCount()
      })
  })
}

// Get last N invalid blocks
export function getLastInvalidBlocks(count: number = 5): Promise<ApiResponse<any>> {
  return new Promise<ApiResponse<any>>((resolve) => {
    incSpinCount()

    let baseUrl = ''
    assetHTTPAddress.subscribe((value) => {
      baseUrl = value
    })()

    fetch(`${baseUrl}/blocks/invalid?count=${count}`, {
      method: 'GET',
      credentials: 'include',
    })
      .then((response) => checkInitialResponse(response))
      .then((data) => {
        resolve({ ok: true, data: data.data })
      })
      .catch((error) => {
        resolve(handleApiError(error, 'getLastInvalidBlocks'))
      })
      .finally(() => {
        decSpinCount()
      })
  })
}

// Convenience function to run the blockchain
export function runFSM(): Promise<ApiResponse<FSMState>> {
  return callApi<FSMState>(`${baseUrl}/fsm/run`, {
    method: 'POST',
    credentials: 'include',
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, '/fsm/run')
    })
    .catch((error) => handleApiError<FSMState>(error, '/fsm/run'))
}

// Convenience function to stop the blockchain (idle)
export function idleFSM(): Promise<ApiResponse<FSMState>> {
  return callApi<FSMState>(`${baseUrl}/fsm/idle`, {
    method: 'POST',
    credentials: 'include',
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, '/fsm/idle')
    })
    .catch((error) => handleApiError<FSMState>(error, '/fsm/idle'))
}

// Convenience function to start catchup mode
export function catchupFSM(): Promise<ApiResponse<FSMState>> {
  return callApi<FSMState>(`${baseUrl}/fsm/catchup`, {
    method: 'POST',
    credentials: 'include',
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, '/fsm/catchup')
    })
    .catch((error) => handleApiError<FSMState>(error, '/fsm/catchup'))
}

// Convenience function to start legacy sync mode
export function legacySyncFSM(): Promise<ApiResponse<FSMState>> {
  return callApi<FSMState>(`${baseUrl}/fsm/legacysync`, {
    method: 'POST',
    credentials: 'include',
  })
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<FSMState>
      }
      return handleApiError<FSMState>(response.error, '/fsm/legacysync')
    })
    .catch((error) => handleApiError<FSMState>(error, '/fsm/legacysync'))
}

// BUMP (BSV Unified Merkle Path) format interfaces
// Based on BRC-74: https://github.com/bitcoin-sv/BRCs/blob/master/transactions/0074.md
export interface BUMPNode {
  offset: number
  hash?: string
  txid?: boolean
  duplicate?: boolean
}

export type BUMPLevel = BUMPNode[]

export interface MerkleProofData {
  blockHeight: number
  path: BUMPLevel[]
}

// Legacy interface kept for backward compatibility
export interface LegacyMerkleProofData {
  txid: string
  blockHash: string
  blockHeight: number
  merkleRoot: string
  subtreeIndex: number
  txIndexInSubtree: number
  subtreeRoot: string
  subtreeProof: string[]
  blockProof: string[]
  completePath: string[]
  flags: string[]
}

// Get merkle proof for a transaction
export function getMerkleProof(txHash: string): Promise<ApiResponse<MerkleProofData>> {
  return get<MerkleProofData>(`${baseUrl}/merkle_proof/${txHash}/json`)
    .then((response) => {
      if (response.ok) {
        return { ok: true, data: response.data } as ApiResponse<MerkleProofData>
      }
      return handleApiError<MerkleProofData>(response.error, `/merkle_proof/${txHash}/json`)
    })
    .catch((error) => handleApiError<MerkleProofData>(error, `/merkle_proof/${txHash}/json`))
}
