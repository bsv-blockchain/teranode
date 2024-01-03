<script lang="ts">
  import { createEventDispatcher } from 'svelte'

  import { Button, FootnoteContainer, Icon, LabelContainer, Typo } from '$lib/components'
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

  let type = 'file'

  export let label: any = ''
  export let footnote: any = ''
  export let footnoteHtml = true
  export let required = false
  export let name = ''
  export let disabled = false
  export let valid = true
  export let error = ''
  export let accept = '*'
  export let multiple = false
  export let titleText: any = ''
  export let hintText: any = ''
  export let selectText: any = ''

  export let compact = false

  export let labelPlacement: LabelPlacementType = LabelPlacement.top
  export let labelAlignment: LabelAlignmentType = LabelAlignment.start

  export let size: InputSizeType = ComponentSize.large
  $: styleSize = getStyleSizeFromComponentSize(size)

  let focused = false

  let inputRef
  let btnRef

  function onInputParentClick() {
    // btnRef.focus()
  }

  function toArray(fileList) {
    return Array.from(fileList)
  }

  function onSelect() {
    inputRef.click()
  }

  function onInputChange(e) {
    const value = toArray(inputRef.files) as File[]
    dispatch('change', { name, type, value })
  }

  let dragOver = false

  function onDrop(e) {
    dragOver = false
    dispatch('change', { name, type, value: toArray(e.dataTransfer.files) as File[] })
  }
  function onDragOver(e) {
    dragOver = true
  }
  function onDragLeave(e) {
    dragOver = false
  }

  function onFocusAction(eventName) {
    switch (eventName) {
      case 'blur':
        focused = false
        break
      case 'focus':
        focused = true
        break
    }
    dispatch(eventName)
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
      `--direction-input:${compact ? 'row' : 'column'}`,
      `--prompt-gap:${compact ? '15px' : '26px'}`,
    ]
  }
</script>

<LabelContainer
  {testId}
  class={`tui-file-upload${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  {size}
  {disabled}
  {label}
  {labelAlignment}
  {labelPlacement}
  {required}
>
  <FootnoteContainer {footnote} {error} {disabled} html={footnoteHtml}>
    <div
      class="input"
      class:disabled
      class:error={!valid || error !== ''}
      class:focused
      class:dragOver
      class:compact
      on:drop|preventDefault={onDrop}
      on:dragenter|preventDefault={onDragOver}
      on:dragover|preventDefault={onDragOver}
      on:dragleave|preventDefault={onDragLeave}
    >
      <input
        bind:this={inputRef}
        {type}
        {name}
        {accept}
        {multiple}
        {disabled}
        on:click={() => (inputRef.value = null)}
        on:input={onInputChange}
        on:focus={() => onFocusAction('focus')}
        on:blur={() => onFocusAction('blur')}
      />
      <div class="prompt">
        <div class="icon">
          <Icon name="feather_upload-cloud" size={44} />
        </div>
        <div class="text">
          <Typo
            variant="body"
            size={3}
            color={disabled ? '#8F8D94' : '#232D7C'}
            value={titleText || 'Drag and drop or select a file'}
          />
          <Typo variant="body" size={4} color="#8F8D94" value={hintText} />
        </div>
      </div>
      <div class="action">
        <Button
          bind:this={btnRef}
          variant="tertiary"
          size="medium"
          {disabled}
          on:click={onSelect}
          tabindex={-1}
        >
          {selectText ? selectText : 'Select file'}
        </Button>
      </div>
    </div>
  </FootnoteContainer>
</LabelContainer>

<style>
  input {
    position: absolute;
    outline: none;
    border: none;
    opacity: 0;
    pointer-events: none;
  }

  .input {
    box-sizing: var(--box-sizing);

    display: flex;
    flex-direction: var(--direction-input);
    align-items: center;
    justify-content: space-between;

    width: 100%;
    height: 260px;
    padding: 42px 25px;

    border-width: var(--border-width);
    border-style: dashed;
    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);
  }
  .input.compact {
    height: 120px;
    padding: 0 25px;
  }
  .input.dragOver {
    background-color: #eff8ff;
    border-color: var(--active-border-color);
  }

  .input:focus,
  .input.focused {
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }

  .input.error,
  .input.error.focused {
    border-color: var(--invalid-border-color);
  }

  .prompt {
    display: flex;
    flex-direction: var(--direction-input);
    align-items: center;
    gap: var(--prompt-gap);
  }
  .icon {
    width: 44px;
    height: 44px;
    color: #232d7c;
  }
  .disabled .icon {
    color: #8f8d94;
  }
  .text {
    display: flex;
    flex-direction: column;
    align-items: center;
  }
  .input.compact .text {
    align-items: flex-start;
  }

  .disabled,
  .disabled:active {
    background-color: #efefef;
    border-color: #8f8d94;
    color: #8f8d94;
  }
</style>
