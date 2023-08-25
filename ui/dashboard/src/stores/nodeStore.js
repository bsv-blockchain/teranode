import { writable } from 'svelte/store';

// Create a writable store
export const nodes = writable([]);
export const blocks = writable([]);
export const error = writable("");

function timeout(ms) {
  return new Promise((_, reject) => setTimeout(() => reject(new Error('Promise timed out')), ms));
}

async function fetchData() {
  try {
    const n = await getNodes();

    await Promise.all(
      n.map(async (node) => {
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

    // Go through all the nodes and get a unique list of hashes. Store these in a map
    // along with the address of the server that has the hash.
    const hs = {};

    n.forEach((node) => { // Use forEach instead of map
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

    // Update the stores
    nodes.set(n);
    blocks.set(blockObject);
    error.set("");

    // Call fetchData() again in 1s
    setTimeout(fetchData, 1000);
  } catch (err) {
    console.error(err)
    error.set(err.message);
  }
}

async function getNodes() {
  const response = await fetch('https://bootstrap.ubsv.dev:8099/nodes');

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

// Call fetchData() once on load
fetchData();
