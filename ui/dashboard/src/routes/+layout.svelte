<script>
  import UpdateSpinner from '@components/UpdateSpinner.svelte'
  import { fetchData, loading, lastUpdated } from '@stores/nodeStore.js'

  let age = ''

  // Create an interval to update the age
  $: {
    setInterval(() => {
      const seconds = Math.floor((new Date() - $lastUpdated) / 1000)
      if (seconds === 1) {
        age = `${seconds} second`
      } else {
        age = `${seconds} seconds`
      }
    }, 500)
  }
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
    <span class="navbar-item has-text-light">
      <span class="is-size-4 client">Teranode</span>
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
  >
    <span aria-hidden="true" />
    <span aria-hidden="true" />
    <span aria-hidden="true" />
  </a>

  <div id="navbarBasicExample" class="navbar-menu">
    <div class="navbar-start">
      <a class="navbar-item" href="/"> Dashboard </a>
      <a class="navbar-item" href="/tree"> Tree </a>
      <a class="navbar-item" href="/treedemo1"> TreeDemo1 </a>
      <a class="navbar-item" href="/treedemo2"> TreeDemo2 </a>
      <a class="navbar-item" href="/viewer"> Viewer </a>
      <a class="navbar-item" href="/blocks"> Blocks </a>
      <a class="navbar-item" href="/blockchain"> Blockchain </a>
    </div>

    <div class="navbar-start">
      <UpdateSpinner spin={$loading} text={age} updateFunc={fetchData} />
    </div>
  </div>
</nav>

<slot />
