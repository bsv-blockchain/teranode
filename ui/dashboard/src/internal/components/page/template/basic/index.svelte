<script lang="ts">
  import { Footer, Header } from '$lib/components'
  import ContentBasic from '../../content/basic/index.svelte'
  import { logos } from '../../../../assets/logos'

  export let testId: string | undefined | null = null
</script>

<Header logoSvg={logos.teranode} showLinks={false} testId="header" />

<div class="content-container" data-test-id={testId}>
  <ContentBasic>
    <slot />
  </ContentBasic>

  <Footer testId="footer" />
</div>

<style>
  .content-container {
    position: absolute;
    top: var(--header-height);
    width: 100%;
    height: calc(100% - var(--header-height));
    overflow-x: hidden;
    overflow-y: auto;
  }
</style>
