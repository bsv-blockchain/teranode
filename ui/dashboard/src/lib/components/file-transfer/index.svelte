<script lang="ts">
  import { createEventDispatcher } from 'svelte'

  import { getFileKey as getKey } from '../../utils/files'
  import { dataSize } from '../../utils/format'
  import { FootnoteContainer, Icon, LabelContainer, Progress, Typo } from '$lib/components'
  import {
    ComponentSize,
    LabelAlignment,
    LabelPlacement,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type { LabelAlignmentType, LabelPlacementType } from '$lib/styles/types'
  import type { InputSizeType } from '$lib/styles/types/input'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let label: any = ''
  export let footnote: any = ''
  export let footnoteHtml = true
  export let name = ''
  export let idField = 'name'
  export let required = false
  export let disabled = false
  export let valid = true
  export let error: any = ''

  export let labelPlacement: LabelPlacementType = LabelPlacement.top
  export let labelAlignment: LabelAlignmentType = LabelAlignment.start

  export let size: InputSizeType = ComponentSize.large
  $: styleSize = getStyleSizeFromComponentSize(size)

  // upload state
  export let onError = (error) => {}
  export let getFileKey = getKey
  export let files: File[] = []
  export let fileStatusData = {} // exposed mostly to allow reactive binding for reading values
  export let fileDataMap = {} // exposed mostly to allow reactive binding for reading values
  export let imageSrcData = {} // exposed mostly to allow reactive binding for reading values
  export let supportedImageSrcDataFileTypes = ['image/png', 'image/jpeg']

  let readerMap: { [key: string]: FileReader } = {}

  $: {
    if (files.length === 0) {
      Object.values(readerMap).forEach((reader) => reader.abort())
      imageSrcData = {}
      fileStatusData = {}
      fileDataMap = {}
      readerMap = {}
      onData()
      onStatus()
    } else {
      files.forEach((file) => {
        const key = getFileKey(file)
        // process image preview
        if (!imageSrcData[key] && supportedImageSrcDataFileTypes.includes(file.type)) {
          const reader = new FileReader()

          reader.onloadend = () => (imageSrcData[key] = reader.result)
          reader.onerror = () => onError(reader.error)
          reader.readAsDataURL(file)
        }
        // read file data
        if (!fileDataMap[key] && !readerMap[key]) {
          const reader = new FileReader()
          readerMap[key] = reader
          fileStatusData[key] = { state: 'progress', progress: 0 }
          onStatus()

          reader.onload = () => {
            fileStatusData[key] = { state: 'success', progress: 1 }
            fileDataMap[key] = reader.result
            onStatus()
            onData()
          }
          reader.onprogress = (data) => {
            if (data.lengthComputable) {
              fileStatusData[key] = {
                state: 'progress',
                progress: data.loaded / data.total,
              }
            }
          }
          reader.onabort = () => {
            fileStatusData[key] = { state: 'cancelled', progress: 1 }
            onStatus()
          }
          reader.onerror = () => {
            fileStatusData[key] = { state: 'failure', progress: 1 }
            onStatus()
            onError(reader.error)
          }
          reader.readAsArrayBuffer(file)
        }
      })
    }
  }

  let focused = false

  function onCancel(file) {
    const key = getFileKey(file)
    readerMap[key].abort()
    dispatch('cancel', { name, value: file })
  }

  function onRemove(file) {
    const key = getFileKey(file)
    files = files.filter((item) => getFileKey(item) !== key)
    if (imageSrcData[key]) {
      delete imageSrcData[key]
    }
    if (fileDataMap[key]) {
      delete fileDataMap[key]
      onData()
    }
    if (fileStatusData[key]) {
      delete fileStatusData[key]
      onStatus()
    }
    if (readerMap[key]) {
      delete readerMap[key]
    }
    dispatch('remove', { name, value: file })
  }

  function onData() {
    dispatch('data', { name, value: fileDataMap })
  }

  function onStatus() {
    dispatch('status', { name, value: fileStatusData })
  }

  function getLabelColor(file) {
    return fileStatusData[getFileKey(file)]?.state === 'failure' ? '#FF344C' : '#282933'
  }

  $: inputVarStr = `--input-default`
  $: inputSizeStr = `--input-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    let states = ['enabled', 'hover', 'active', 'focus', 'disabled']
    cssVars = [
      ...states.reduce(
        (acc, state) => [
          ...acc,
          `--${state}-color:var(${inputVarStr}-${state}-color)`,
          `--${state}-bg-color:var(${inputVarStr}-${state}-bg-color)`,
          `--${state}-border-color:var(${inputVarStr}-${state}-border-color)`,
        ],
        [] as string[],
      ),
      `--invalid-border-color:var(--input-default-invalid-border-color)`,
      `--border-radius:var(${inputSizeStr}-border-radius)`,
      `--border-width:var(--comp-border-width)`,
    ]
  }
</script>

<LabelContainer
  {testId}
  class={`tui-file-transfer${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  {size}
  {disabled}
  {label}
  {labelAlignment}
  {labelPlacement}
  {required}
>
  <FootnoteContainer {footnote} {error} {disabled} html={footnoteHtml}>
    <div class="content" class:disabled class:error={!valid || error !== ''} class:focused>
      {#each files as file (file[idField])}
        <div
          class="item"
          class:success={fileStatusData[getFileKey(file)] &&
            fileStatusData[getFileKey(file)].state === 'success'}
        >
          <div class="info">
            <div class="icon">
              {#if imageSrcData[getFileKey(file)]}
                <img
                  src={imageSrcData[getFileKey(file)]}
                  style="width: 24px; height: 24px;"
                  alt="thumb-preview"
                />
              {:else}
                <Icon name="bi_file-earmark-image" size={24} />
              {/if}
            </div>
            <div class="text">
              <div class="labels">
                <Typo variant="body" size={4} value={file.name} color={getLabelColor(file)} />
                <Typo
                  variant="body"
                  size={4}
                  value={dataSize(file.size)}
                  color={getLabelColor(file)}
                />
              </div>
              {#if fileStatusData[getFileKey(file)] && fileStatusData[getFileKey(file)].progress < 1}
                <Progress
                  ratio={fileStatusData[getFileKey(file)]
                    ? fileStatusData[getFileKey(file)].progress
                    : 0}
                  size="small"
                  normalColor="#232D7C"
                />
              {/if}
            </div>
          </div>
          {#if fileStatusData[getFileKey(file)]}
            {#if fileStatusData[getFileKey(file)].progress < 1}
              <!-- svelte-ignore a11y-click-events-have-key-events -->
              <div class="action" on:click={() => onCancel(file)}>
                <Icon name="close" size={18} />
              </div>
            {:else}
              <!-- svelte-ignore a11y-click-events-have-key-events -->
              <div class="action" on:click={() => onRemove(file)}>
                <Icon name="trash" size={18} />
              </div>
            {/if}
          {/if}
        </div>
      {/each}
    </div>
  </FootnoteContainer>
</LabelContainer>

<style>
  .content {
    box-sizing: var(--box-sizing);

    width: 100%;
    min-height: 120px;
    padding: 25px;

    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 20px;

    border-width: var(--border-width);
    border-style: solid;
    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);
  }

  .content:focus,
  .content.focused {
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }

  .content.error,
  .content.error.focused {
    border-color: var(--invalid-border-color);
  }

  .disabled,
  .disabled:active {
    color: var(--disabled-border-color);
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
  }

  .item {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .info {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .icon {
    width: 24px;
    height: 24px;
    color: rgba(0, 148, 255, 0.5);
    opacity: 0.5;
  }
  .text {
    width: 80%;
    text-align: left;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    justify-content: center;
    gap: 8px;
  }
  .labels {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
    opacity: 0.5;
  }
  .item.success .icon {
    opacity: 1;
  }
  .item.success .labels {
    opacity: 1;
  }
  .action {
    width: 18px;
    height: 18px;
    color: var(--focus-border-color);
  }
  .action:hover {
    cursor: pointer;
  }
  .disabled .action,
  .disabled .action:hover {
    cursor: auto;
    color: var(--disabled-color);
  }
</style>
