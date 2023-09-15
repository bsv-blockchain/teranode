<script>
  import { onMount } from 'svelte'
  import UpdateSpinner from '@components/UpdateSpinner.svelte'
  import {
    nodes,
    connectToBootstrap,
    selectedNode,
  } from '@stores/bootstrapStore.js'

  import {
    loading,
    lastUpdated,
    connectToBlobServer,
  } from '@stores/nodeStore.js'

  let isActive = false

  function toggleNavbar() {
    isActive = !isActive
  }

  function handleNavbarItemClick() {
    isActive = false
  }

  let age = 0
  let cancel = null

  // Create an interval to update the age
  $: {
    if (cancel) {
      clearInterval(cancel)
    }

    age = 0

    cancel = setInterval(() => {
      age = Math.floor((new Date() - $lastUpdated) / 1000)
    }, 500)
  }

  onMount(async () => {
    connectToBootstrap($selectedNode)
    connectToBlobServer($selectedNode)
  })
</script>

<svelte:head>
  <!-- Include Bulma CSS for the entire app -->
  <link
    rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bulma@0.9.3/css/bulma.min.css"
  />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link
    href="https://fonts.googleapis.com/css2?family=Roboto+Mono&display=swap"
    rel="stylesheet"
  />
  <link rel="preload" href="https://d3js.org/d3.v6.min.js" as="script" />
  <script src="https://d3js.org/d3.v6.min.js" defer></script>

  <style>
    body {
      font-family: 'Roboto Mono', monospace;
      background-color: #f4f4f4;
      margin: 0;
      padding: 0;
      padding-top: 52px; /* Adjust for the height of the fixed navbar */
    }

    /* Custom darker blue color */
    .navbar.is-primary {
      background-color: #004466; /* Change this value to your preferred shade of dark blue */
    }

    /* Add more global styles as needed */
  </style>
</svelte:head>

<nav class="navbar is-fixed-top is-dark" aria-label="main navigation">
  <div class="navbar-brand">
    <span class="navbar-item">
      <a class="is-size-4 client" href="/">Teranode</a>
    </span>
  </div>

  <!-- svelte-ignore a11y-invalid-attribute -->
  <a
    role="button"
    class="navbar-burger"
    href="#"
    aria-label="menu"
    aria-expanded="false"
    data-target="navbarBasicExample"
    on:click={toggleNavbar}
    class:is-active={isActive}
  >
    <span aria-hidden="true" />
    <span aria-hidden="true" />
    <span aria-hidden="true" />
  </a>

  <div id="navbarBasicExample" class="navbar-menu" class:is-active={isActive}>
    <div class="navbar-start">
      <div class="navbar-item">
        <div class="select">
          <select
            bind:value={$selectedNode}
            on:change={() => connectToBootstrap($selectedNode)}
          >
            <option disabled>Select a URL</option>
            {#each $nodes as node (node.blobServerHTTPAddress)}
              <option value={node.blobServerHTTPAddress}
                >{new URL(node.blobServerHTTPAddress).hostname}</option
              >
            {/each}
          </select>
        </div>
      </div>

      <a class="navbar-item" href="/viewer" on:click={handleNavbarItemClick}>
        Viewer
      </a>
      <a class="navbar-item" href="/txviewer" on:click={handleNavbarItemClick}>
        UTXOInspector
      </a>
      <!-- <a class="navbar-item" href="/blocks" on:click={handleNavbarItemClick}> Blocks </a> -->
      <a
        class="navbar-item"
        href="/blockchain"
        on:click={handleNavbarItemClick}
      >
        Blockchain
      </a>
      <a class="navbar-item" href="/listener" on:click={handleNavbarItemClick}>
        Listener
      </a>
      <a class="navbar-item" href="/chain" on:click={handleNavbarItemClick}>
        Chain
      </a>
    </div>

    <div class="navbar-end">
      <div class="navbar-item">
        <UpdateSpinner flash={$loading} text={age} />
      </div>
    </div>
  </div>
</nav>

<slot />

<style>
  .client {
    color: white;
  }
</style>
