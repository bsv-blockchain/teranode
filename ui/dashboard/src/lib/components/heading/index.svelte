<svelte:options runes={true} />

<script lang="ts">
  import { mediaSize, MediaSize } from '../../stores/media'
  import { Typo } from '$lib/components'

  let {
    testId = null,
    size = 1,
    html = false,
    value = '',
  }: {
    testId?: string | undefined | null
    size?: number
    html?: boolean
    value?: any
  } = $props()

  const mediaSmall = $derived($mediaSize <= MediaSize.sm)

  const responsiveSize = $derived.by(() => {
    switch (size) {
      case 1:
        return mediaSmall ? 2 : 1
      case 2:
        return mediaSmall ? 3 : 2
      case 3:
        return mediaSmall ? 4 : 3
      default:
        return size
    }
  })
</script>

<Typo variant="heading" size={responsiveSize} {value} {html} {testId} />
