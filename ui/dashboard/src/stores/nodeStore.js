import { writable, get } from 'svelte/store';

const BOOTSTRAP_SERVER="https://bootstrap.ubsv.dev:8099"
// const BOOTSTRAP_SERVER="https://localhost:8099"

// Create a writable store
export const nodes = writable([]);
export const blocks = writable([]);
export const error = writable("");
export const loading = writable(false);

let cancelFunction = null;

function timeout(ms) {
  return new Promise((_, reject) => setTimeout(() => reject(new Error('Promise timed out')), ms));
}

async function fetchData(force = false) {
  if (!force && get(loading) === true) {
    setTimeout(fetchData(true), 10000); // Try again in 10s
    return;
  }

  try {
    loading.set(true);

    if (cancelFunction) {
      cancelFunction();
    }

    const n = await getNodes();

    await decorateNodesWithHeaders(n);

    const b = await getBestBlocks(n);


    // Start a websocket connection to the first node
    cancelFunction = connectToWebSocket(n[0])

    // Update the stores
    nodes.set(n);
    blocks.set(b);
    error.set("");

    // Call fetchData() again in 1s
    setTimeout(fetchData, 1000);
  } catch (err) {
    console.error(err)
    error.set(err.message);
  } finally {
    loading.set(false);
  }
}

async function getNodes() {
  const response = await fetch(`${BOOTSTRAP_SERVER}/nodes`);

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`);
  }

  const n = await response.json();

  return n.sort((a, b) => {
    if (a.name < b.name) return -1;
    if (a.name > b.name) return 1;
    return 0;
  });
}

async function decorateNodesWithHeaders(nodes) {
  await Promise.all(
    nodes.map(async (node) => {
      if (node.blobServerHTTPAddress) {
        try {
          const header = await Promise.race([
            getBestBlockHeader(node.blobServerHTTPAddress),
            timeout(1000)
          ]);
          node.header = header; // Add the header directly to the node
        } catch (error) {
          console.error(`Error fetching header for node ${node.id}:`, error.message);
          node.header = { error: "timeout" }; // Add the error directly to the node
        }
      } else {
        node.header = {}; // Add an empty header object if no blobServerHTTPAddress
      }
    })
  );
}

async function getBestBlocks(nodes) {
  // Go through all the nodes and get a unique list of hashes. Store these in a map
  // along with the address of the server that has the hash.
  const hs = {};

  nodes.forEach((node) => { // Use forEach instead of map
    if (node.header && node.header.hash && node.blobServerHTTPAddress) {
      hs[node.header.hash] = node.blobServerHTTPAddress;
    }
  });

  const b = await Promise.all(
    Object.entries(hs).map(async ([hash, addr]) => {
      try {
        const blocks = await Promise.race([
          getLast10Blocks(hash, addr),
          timeout(1000)
        ]);

        return {
          hash,
          blocks
        }
      } catch (error) {
        console.error(`Error fetching blocks for hash ${hash}:`, error.message); // Corrected the error message
        return {
          hash,
          blocks: { error: "timeout" }
        }
      }
    })
  );

  // Convert the array to an object with hash as the key and blocks as the value
  const blockObject = b.reduce((acc, { hash, blocks }) => {
    acc[hash] = blocks;
    return acc;
  }, {});

  // if (Object.entries(blockObject).length > 1) {
  //   console.log(JSON.stringify(blockObject, null, 2))
  // }

  return blockObject;
}


async function getBestBlockHeader(address) {
  const url = `${address}/bestblockheader/json`
  // console.log("Fetching", url)
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`);
  }

  return await response.json();
}

async function getLast10Blocks(hash, address) {
  const url = `${address}/headers/${hash}/json?n=10`
  // console.log("Fetching", url)
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`);
  }

  return await response.json();
}

function connectToWebSocket(node) {
  const url = new URL(node.blobServerHTTPAddress);

  const wsUrl = `wss://${url.host}/ws`


  const socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    console.log(`WebSocket connection opened to ${url}`);
  };

  socket.onmessage = async (event) => {
    try {
      const data = await event.data.text()
      const json = JSON.parse(data)
      console.log(json)
      if (json.type === 'Block') {  // Block
        setTimeout(fetchData, 0);
      }
    } catch (error) {
      console.error('Error parsing WebSocket data:', error);
    }
  };

  socket.onclose = () => {
    console.log(`WebSocket connection closed by server (${url})`);
    // Reconnect logic can be added here if needed
  };

  // Cleanup function
  return () => {
    console.log(`WebSocket connection closed by client (${url})`);
    socket.close();
  };
}


// Call fetchData() once on load
fetchData();
