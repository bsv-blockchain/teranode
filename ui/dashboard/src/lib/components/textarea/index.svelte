<script lang="ts">
  import { createEventDispatcher, onMount, afterUpdate } from 'svelte'
  import { drag } from '../../actions/drag'
  import { clamp } from '../../utils/numbers'
  import { FootnoteContainer, Icon, LabelContainer } from '$lib/components'
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

  //   export let type = 'text'
  let type = 'text'

  export let label: any = ''
  export let footnote: any = ''
  export let footnoteHtml = true
  export let required = false
  export let name = ''
  export let value = ''
  export let placeholder = ''
  export let disabled = false
  export let readonly = false
  export let valid = true
  export let error = ''

  export let resize = true
  export let minH = 80
  export let maxH = 400

  export let labelPlacement: LabelPlacementType = LabelPlacement.top
  export let labelAlignment: LabelAlignmentType = LabelAlignment.start

  export let size: InputSizeType = ComponentSize.large
  $: styleSize = getStyleSizeFromComponentSize(size)

  let height = 120
  let nativeHeight = 60

  $: {
    switch (size) {
      case 'large':
        height = clamp(120, minH, maxH)
        break
      case 'medium':
        height = clamp(108, minH, maxH)
        break
      case 'small':
        height = clamp(84, minH, maxH)
        break
    }
  }

  let focused = false

  let inputRef

  function onInputParentClick() {
    inputRef.focus()
  }

  function onInputChange(e) {
    value = e.srcElement.value
    dispatch('change', { name, type, value })
    updateSize()
  }

  afterUpdate(() => {
    updateSize()
  })

  onMount(() => {
    dispatch('mount', { inputRef })
    updateSize()
  })

  function updateSize() {
    inputRef.parentNode.dataset.replicatedValue = inputRef.value
    nativeHeight = inputRef.scrollHeight - 4
  }

  function onKeyUp(e) {
    updateSize()
  }

  let dragStartH: number | null = null

  function dragStart(e) {
    dragStartH = height
  }

  function dragMove(e) {
    if (!dragStartH) {
      return
    }
    const { x, y } = e.detail
    let newHeight = dragStartH + y
    newHeight = clamp(newHeight, minH, maxH)

    height = newHeight
  }

  function dragStop() {
    dragStartH = null
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
  $: textareaSizeStr = `--textarea-size-${styleSize}`

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
      `--height:${height}px`,
      `--padding:var(${textareaSizeStr}-padding)`,
      `--border-radius:var(${inputSizeStr}-border-radius)`,
      `--icon-size:var(${inputSizeStr}-icon-size)`,
      `--font-size:var(${inputSizeStr}-font-size)`,
      `--line-height:var(${inputSizeStr}-line-height)`,
      `--letter-spacing:var(${inputSizeStr}-letter-spacing)`,
      `--font-weight:var(--input-font-weight)`,
      `--border-width:var(--comp-border-width)`,
      `--gap:var(--button-icon-gap, 6px)`,
    ]
  }
</script>

<LabelContainer
  {testId}
  class={`tui-textarea${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  {size}
  {disabled}
  {label}
  {labelAlignment}
  {labelPlacement}
  {required}
>
  <FootnoteContainer {footnote} {error} {disabled} html={footnoteHtml}>
    <div>
      <!-- svelte-ignore a11y-click-events-have-key-events -->
      <div
        class="input"
        class:disabled
        class:error={!valid || error !== ''}
        class:focused
        class:resize
        class:dragging={dragStartH === null}
        on:click={disabled ? null : onInputParentClick}
      >
        <!-- svelte-ignore a11y-no-noninteractive-tabindex -->
        <div class="wrap">
          <div class="control-container" tabindex={-1}>
            <textarea
              bind:this={inputRef}
              {type}
              {name}
              {value}
              {placeholder}
              {disabled}
              {readonly}
              on:input={disabled ? null : onInputChange}
              on:focus={disabled ? null : () => onFocusAction('focus')}
              on:blur={disabled ? null : () => onFocusAction('blur')}
              on:keyup={onKeyUp}
            />
          </div>
        </div>
        <div
          class="resize-icon"
          use:drag
          on:dragstart={disabled ? null : dragStart}
          on:dragmove={dragMove}
          on:dragstop={dragStop}
        >
          <Icon name="drag-corner" size={14} />
        </div>
      </div>
    </div>
  </FootnoteContainer>
</LabelContainer>

<style>
  .wrap {
    width: 100%;
    height: 100%;
    overflow-x: hidden;
    overflow-y: auto;
  }

  textarea {
    box-sizing: var(--box-sizing);

    outline: none;
    border: none;
    width: 100%;
    background-color: inherit;

    font-family: var(--font-family);
  }

  /* See: https://css-tricks.com/the-cleanest-trick-for-autogrowing-textareas/ */
  .control-container {
    width: 100%;
    display: grid;
  }
  .control-container::after {
    content: attr(data-replicated-value) ' ';
    white-space: pre-wrap;
    visibility: hidden;
  }
  .control-container > textarea {
    resize: none;
    overflow: hidden;
  }
  .control-container > textarea,
  .control-container::after {
    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);

    grid-area: 1 / 1 / 2 / 2;
  }

  .resize-icon {
    position: absolute;
    right: 1px;
    bottom: 3px;
    color: var(--focus-color);
    cursor: auto;
  }

  .input {
    box-sizing: var(--box-sizing);

    position: relative;
    display: flex;
    align-items: center;
    padding: var(--padding);
    height: var(--height);

    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);

    border-width: var(--border-width);
    border-style: solid;
    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);
  }
  .input.resize .resize-icon {
    color: var(--enabled-color);
    cursor: ns-resize;
  }

  .input.focused {
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }
  .input.focused.resize .resize-icon {
    color: var(--focus-border-color);
  }

  .input.error,
  .input.error.focused {
    border-color: var(--invalid-border-color);
  }

  .disabled,
  .disabled:active {
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
  }
  .input.disabled.resize .resize-icon {
    color: var(--disabled-border-color);
    cursor: auto;
  }
</style>
