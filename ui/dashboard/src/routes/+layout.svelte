<script>
  import { onMount } from 'svelte'
  import {
    loading,
    lastUpdated,
    connectToBlobServer,
    blobServerHTTPAddress,
  } from '@stores/nodeStore.js'

  import { connectToP2PServer } from '@stores/p2pStore.js'

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
    connectToBlobServer($blobServerHTTPAddress)
    connectToP2PServer()
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
      <a class="is-size-4 client centre no-highlight" href="/">
        <img draggable="false" src="/teranode_shadow.png" alt="Teranode" />
      </a>
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
      <!-- <a
        class="navbar-item"
        href="/blockchain"
        on:click={handleNavbarItemClick}
      >
        Blockchain
      </a> -->
      <a class="navbar-item" href="/viewer" on:click={handleNavbarItemClick}>
        Viewer
      </a>
      <a
        class="navbar-item"
        href="/utxoviewer"
        on:click={handleNavbarItemClick}
      >
        UTXOInspector
      </a>
      <!-- <a class="navbar-item" href="/blocks" on:click={handleNavbarItemClick}> Blocks </a> -->
      <!-- <a class="navbar-item" href="/listener" on:click={handleNavbarItemClick}>Listener</a> -->
      <a class="navbar-item" href="/chain" on:click={handleNavbarItemClick}>
        Chain
      </a>
      <a class="navbar-item" href="/metrics" on:click={handleNavbarItemClick}>
        Metrics
      </a>

      <a class="navbar-item" href="/p2p" on:click={handleNavbarItemClick}>
        P2P
      </a>

      <a class="navbar-item" href="/network" on:click={handleNavbarItemClick}>
        Network
      </a>
    </div>
  </div>
</nav>

<slot />

<style>
  .client {
    color: white;
  }

  .centre {
    display: flex;
    align-items: center;
  }

  .no-highlight {
    user-select: none;
    -webkit-user-select: none; /* Safari */
    -moz-user-select: none; /* Firefox */
    -ms-user-select: none; /* IE/Edge */
  }
</style>
