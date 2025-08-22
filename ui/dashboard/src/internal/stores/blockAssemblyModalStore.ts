import { writable } from 'svelte/store'
import type { BlockAssemblyDetails } from '../components/msgbox/types'

interface BlockAssemblyModalState {
  isOpen: boolean
  nodeId: string | null
  nodeUrl: string | null
  blockAssembly: BlockAssemblyDetails | null
}

function createBlockAssemblyModalStore() {
  const { subscribe, set, update } = writable<BlockAssemblyModalState>({
    isOpen: false,
    nodeId: null,
    nodeUrl: null,
    blockAssembly: null,
  })

  return {
    subscribe,
    show: (nodeId: string, nodeUrl: string, blockAssembly: BlockAssemblyDetails) => {
      set({
        isOpen: true,
        nodeId,
        nodeUrl,
        blockAssembly,
      })
    },
    hide: () => {
      set({
        isOpen: false,
        nodeId: null,
        nodeUrl: null,
        blockAssembly: null,
      })
    },
  }
}

export const blockAssemblyModalStore = createBlockAssemblyModalStore()