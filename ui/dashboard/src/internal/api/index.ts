import { spinCount } from '../stores/nav'
import { assetHTTPAddress } from '$internal/stores/nodeStore'

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

function checkInitialResponse(response) {
  // FIXME
  // eslint-disable-next-line
  return new Promise<{ data: any }>(async (resolve, reject) => {
    if (response.ok) {
      let data = null

      const mimeType = response.headers.get('Content-Type') || 'text/plain'
      if (mimeType.toLowerCase().startsWith('application/json')) {
        try {
          data = await response.json()
        } catch (e) {
          data = null
        }
      } else if (mimeType.toLowerCase().startsWith('application/octet-stream')) {
        try {
          data = await response.blob()
        } catch (e) {
          data = null
        }
      } else {
        data = await response.text()
      }

      resolve({ data })
    } else {
      let errorBody: any = null
      try {
        errorBody = await response.json()
      } catch (e) {
        errorBody = null
      }

      reject({
        code: response.status,
        message: errorBody?.error || response.statusText || 'Unspecified error.',
      })
    }
  })
}

function callApi(url, options: any = {}, done?, fail?) {
  if (!options.method) {
    options.method = 'GET'
  }
  incSpinCount()

  return fetch(url, options)
    .then(async (res) => {
      const { data } = await checkInitialResponse(res)
      return data
    })
    .then((data) => {
      if (done) {
        done(data)
      }
      return { ok: true, data }
    })
    .catch((error) => {
      console.log(error)
      if (fail) {
        fail(error.message)
      }
      return { ok: false, error }
    })
    .finally(decSpinCount)
}

function get(url, options: any = {}, done?, fail?) {
  const { query, ...rest } = options
  const theUrl = query ? url + '?' + new URLSearchParams(query) : url
  return callApi(theUrl, { options: rest, method: 'GET' }, done, fail)
}

// function post(url, options = {}, done?, fail?) {
//   return callApi(url, { ...options, method: 'POST' }, done, fail)
// }

// function put(url, options = {}, done?, fail?) {
//   return callApi(url, { ...options, method: 'PUT' }, done, fail)
// }

// function del(url, options = {}, done?, fail?) {
//   return callApi(url, { ...options, method: 'DELETE' }, done, fail)
// }

//let baseUrl = ''
const baseUrl = 'https://m1.scaling.ubsv.dev/api/v1'

// assetHTTPAddress.subscribe((value) => {
//   baseUrl = value
// })

export const getItemApiUrl = (type: ItemType, hash: string) => {
  return `${baseUrl}/${type}/${hash}/json`
}

// api methods

export function getLastBlocks(data = {}, done?, fail?) {
  return get(`${baseUrl}/lastblocks?${new URLSearchParams(data)}`, {}, done, fail)
}

export function getItemData(data: { type: ItemType; hash: string }, done?, fail?) {
  return get(getItemApiUrl(data.type, data.hash), {}, done, fail)
}

export function getBlocks(data: { offset: number; limit: number }, done?, fail?) {
  return get(`${baseUrl}/blocks`, { query: { offset: data.offset, limit: data.limit } }, done, fail)
}

export function getBestBlockHeaderFromServer(data: { baseUrl: string }, done?, fail?) {
  return get(`${data.baseUrl}/bestblockheader/json`, {}, done, fail)
}

export function getBlockSubtrees(
  data: { hash: string; offset: number; limit: number },
  done?,
  fail?,
) {
  return get(
    `${baseUrl}/block/${data.hash}/subtrees/json`,
    { query: { offset: data.offset, limit: data.limit } },
    done,
    fail,
  )
}

export function getSubtreeTxs(data: { hash: string; offset: number; limit: number }, done?, fail?) {
  return get(
    `${baseUrl}/subtree/${data.hash}/txs/json`,
    { query: { offset: data.offset, limit: data.limit } },
    done,
    fail,
  )
}

export function searchItem(data: { q: string }, done?, fail?) {
  return get(`${baseUrl}/search?${new URLSearchParams(data)}`, {}, done, fail)
}

export function getBlockStats(done?, fail?) {
  return get(`${baseUrl}/blockstats`, {}, done, fail)
}

export function getBlockForks(
    data: { hash: string; limit: number },
    done?,
    fail?,
) {
  return get(
      `${baseUrl}/block/${data.hash}/forks`,
      { query: { limit: data.limit } },
      done,
      fail,
  )
}

export function getBlockGraphData(data: { period: string }, done?, fail?) {
  return get(`${baseUrl}/blockgraphdata/${data.period}`, {}, done, fail)
}
