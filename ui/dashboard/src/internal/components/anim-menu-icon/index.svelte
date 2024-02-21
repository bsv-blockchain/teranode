<script lang="ts">
  // TODO component is a mess, redo later

  export let open = false
  let size = 24

  $: scaleLocal = (18 / 60) * (size / 24)
</script>

<div class="anim-menu-icon" style:--scale={scaleLocal} style:--size={`${size}px`}>
  <div class="container" class:open>
    <div class="icon" class:open>
      <span></span>
      <span></span>
      <span></span>
    </div>
  </div>
</div>

<style>
  .anim-menu-icon {
    box-sizing: var(--box-sizing);
    position: relative;

    width: var(--size);
    height: var(--size);
  }

  .container {
    width: 100%;
    height: 100%;
  }

  .icon {
    margin: 0;
    padding: 0;

    width: 100%;
    height: 100%;

    transform: rotate(0deg) scale(var(--scale)) translate(-18px, -9px);
    transition: var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
    cursor: pointer;
  }

  .icon span {
    position: absolute;
    height: 8px;
    width: 60px;
    background: currentColor;
    border-radius: 8px;
    opacity: 1;
    left: 0;
    top: 1;
    transform: rotate(0deg);
    transition: var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }

  .icon span:nth-child(1) {
    top: 0px;
    transform-origin: left center;
  }

  .icon span:nth-child(2) {
    top: 18px;
    left: 15px;
    width: 45px;
    transform-origin: left center;
  }

  .icon span:nth-child(3) {
    top: 36px;
    transform-origin: left center;
  }

  .icon.open span:nth-child(1) {
    transform: rotate(45deg);
    top: -3px;
    left: 8px;
  }

  .icon.open span:nth-child(2) {
    width: 0%;
    left: 60px;
    opacity: 0;
  }

  .icon.open span:nth-child(3) {
    transform: rotate(-45deg);
    top: 39px;
    left: 8px;
  }
</style>
