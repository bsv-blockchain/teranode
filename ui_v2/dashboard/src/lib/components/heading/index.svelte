<script lang="ts">
  import { mediaSize, MediaSize } from '../../stores/media'
  import { Typo } from '$lib/components'

  export let testId: string | undefined | null = null

  export let size = 1
  export let html = false
  export let value: any = ''

  $: mediaSmall = $mediaSize <= MediaSize.sm

  let responsiveSize = size

  $: {
    switch (size) {
      case 1:
        responsiveSize = mediaSmall ? 2 : 1
        break
      case 2:
        responsiveSize = mediaSmall ? 3 : 2
        break
      case 3:
        responsiveSize = mediaSmall ? 4 : 3
        break
      default:
        responsiveSize = size
    }
  }
</script>

<Typo variant="heading" size={responsiveSize} {value} {html} {testId} />
