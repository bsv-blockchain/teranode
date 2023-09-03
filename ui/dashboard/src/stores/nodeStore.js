import { writable, get } from 'svelte/store';

const BOOTSTRAP_SERVER = "https://bootstrap.ubsv.dev:8099";
// const BOOTSTRAP_SERVER = "https://localhost:8099";
const LOCAL = true;

// Create writable stores
export const nodes = writable([]);
export const blocks = writable([]);
export const error = writable("");
export const loading = writable(false);
export const lastUpdated = writable(new Date());

let cancelFunction = null;

// Promise to resolve after a certain time for timeout handling
function timeout(ms) {
  return new Promise((_, reject) => setTimeout(() => reject(new Error('Promise timed out')), ms));
}

// Retry fetchData after a delay if already loading
async function retryFetchData() {
  if (!get(loading)) {
    setTimeout(fetchData, 10000); // Try again in 10s
  }
}

export async function fetchData(force = false) {
  if (!force && get(loading)) {
    retryFetchData();
    return;
  }

  try {
    loading.set(true);

    if (cancelFunction) {
      cancelFunction();
    }

    const nodesData = await getNodes();
    await decorateNodesWithHeaders(nodesData);
    const bestBlocks = await getBestBlocks(nodesData);

    // Start a websocket connection to the first node.  This will cause the
    // dashboard to update when a new block is received.
    cancelFunction = connectToWebSocket(nodesData[0]);

    // Update stores
    nodes.set(nodesData);
    blocks.set(bestBlocks);
    error.set("");
    lastUpdated.set(new Date());

    // Schedule next automatic fetchData call
    setTimeout(fetchData, 10000);
  } catch (err) {
    console.error(err);
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

  const nodesData = await response.json();

  return nodesData.sort((a, b) => a.name.localeCompare(b.name));
}

async function decorateNodesWithHeaders(nodesData) {
  await Promise.all(
    nodesData.map(async (node) => {
      if (node.blobServerHTTPAddress) {
        try {
          const header = await Promise.race([
            getBestBlockHeader(node.blobServerHTTPAddress),
            timeout(1000)
          ]);
          node.header = header || { error: "timeout" };
        } catch (error) {
          console.error(`Error fetching header for node ${node.id}:`, error.message);
          node.header = { error: "timeout" };
        }
      } else {
        node.header = {};
      }
    })
  );
}

async function getBestBlocks(nodesData) {
  const hashesToAddresses = {};

  nodesData.forEach((node) => {
    if (node.header?.hash && node.blobServerHTTPAddress) {
      hashesToAddresses[node.header.hash] = node.blobServerHTTPAddress;
    }
  });

  const blocksData = await Promise.all(
    Object.entries(hashesToAddresses).map(async ([hash, addr]) => {
      try {
        const blocks = await Promise.race([
          getLast10Blocks(hash, addr),
          timeout(1000)
        ]);

        return {
          hash,
          blocks
        };
      } catch (error) {
        console.error(`Error fetching blocks for hash ${hash}:`, error.message);
        return {
          hash,
          blocks: { error: "timeout" }
        };
      }
    })
  );

  const blockObject = blocksData.reduce((acc, { hash, blocks }) => {
    acc[hash] = blocks;
    return acc;
  }, {});

  return blockObject;
}

async function getBestBlockHeader(address) {
  const url = `${address}/bestblockheader/json`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`);
  }

  return await response.json();
}

async function getLast10Blocks(hash, address) {
  const url = `${address}/headers/${hash}/json?n=10`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`);
  }

  return await response.json();
}

function connectToWebSocket(node) {
  const url = new URL(node.blobServerHTTPAddress);
  const wsUrl = LOCAL ? 'wss://localhost:8090/ws' : `wss://${url.host}/ws`;

  let socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    console.log(`WebSocket connection opened to ${wsUrl}`);
  };

  socket.onmessage = async (event) => {
    try {
      const data = await event.data.text();
      const json = JSON.parse(data);
      if (json.type === 'Block') {
        setTimeout(fetchData, 0);
      }
    } catch (error) {
      console.error('Error parsing WebSocket data:', error);
    }
  };

  socket.onclose = () => {
    console.log(`WebSocket connection closed by server (${wsUrl})`);
    socket = null;
    // Reconnect logic can be added here if needed
  };

  return () => {
    if (socket) {
      console.log(`WebSocket connection closed by client (${wsUrl})`);
      socket.close();
    }
  };
}

// Call fetchData() once on load
fetchData();
