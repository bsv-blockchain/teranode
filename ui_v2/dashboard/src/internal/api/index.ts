import { env } from '$env/dynamic/public'
import { spinCount } from '../stores/nav'
import { assetHTTPAddress } from '$internal/stores/nodeStore'

console.log('env.PUBLIC_BASE_URL = ', env.PUBLIC_BASE_URL)

// const PUBLIC_BASE_URL = env.PUBLIC_BASE_URL

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

function get(url, options = {}, done?, fail?) {
  return callApi(url, { ...options, method: 'GET' }, done, fail)
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

let baseUrl = ''

assetHTTPAddress.subscribe((value) => {
  baseUrl = value
})

// api methods

export function getLastBlocks(data = {}, done?, fail?) {
  return get(`${baseUrl}/lastblocks?${new URLSearchParams(data)}`, {}, done, fail)
}

export function getItemData(data: { type: ItemType; hash: string }, done?, fail?) {
  return get(`${baseUrl}/${data.type}/${data.hash}/json`, {}, done, fail)
}

export function searchItem(data: { q: string }, done?, fail?) {
  return get(`${baseUrl}/search?${new URLSearchParams(data)}`, {}, done, fail)
}
