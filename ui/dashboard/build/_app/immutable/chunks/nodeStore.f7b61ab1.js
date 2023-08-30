import { w as writable } from "./index.7880fa96.js";
const BOOTSTRAP_SERVER = "https://localhost:8099";
const nodes = writable([]);
const blocks = writable([]);
const error = writable("");
const loading = writable(false);
function timeout(ms) {
  return new Promise((_, reject) => setTimeout(() => reject(new Error("Promise timed out")), ms));
}
async function fetchData() {
  try {
    loading.set(true);
    const n = await getNodes();
    await Promise.all(
      n.map(async (node) => {
        if (node.blobServerHTTPAddress) {
          try {
            const header = await Promise.race([
              getBestBlockHeader(node.blobServerHTTPAddress),
              timeout(1e3)
            ]);
            node.header = header;
          } catch (error2) {
            console.error(`Error fetching header for node ${node.id}:`, error2.message);
            node.header = { error: "timeout" };
          }
        } else {
          node.header = {};
        }
      })
    );
    const hs = {};
    n.forEach((node) => {
      if (node.header && node.header.hash && node.blobServerHTTPAddress) {
        hs[node.header.hash] = node.blobServerHTTPAddress;
      }
    });
    const b = await Promise.all(
      Object.entries(hs).map(async ([hash, addr]) => {
        try {
          const blocks2 = await Promise.race([
            getLast10Blocks(hash, addr),
            timeout(1e3)
          ]);
          return {
            hash,
            blocks: blocks2
          };
        } catch (error2) {
          console.error(`Error fetching blocks for hash ${hash}:`, error2.message);
          return {
            hash,
            blocks: { error: "timeout" }
          };
        }
      })
    );
    const blockObject = b.reduce((acc, { hash, blocks: blocks2 }) => {
      acc[hash] = blocks2;
      return acc;
    }, {});
    nodes.set(n);
    blocks.set(blockObject);
    error.set("");
    setTimeout(fetchData, 1e4);
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
  const n = await response.json();
  return n.sort((a, b) => {
    if (a.name < b.name)
      return -1;
    if (a.name > b.name)
      return 1;
    return 0;
  });
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
fetchData();
export {
  blocks as b,
  error as e,
  loading as l,
  nodes as n
};
//# sourceMappingURL=nodeStore.f7b61ab1.js.map
