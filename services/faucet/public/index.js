async function postData(url = '', data = {}) {
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    });

    if (!response.ok) {
      throw new Error(await response.text());
    }

    return await response.json();
  } catch (error) {
    return { error: true, message: error.message };
  }
}


window.onload = function() {
  const faucetForm = document.getElementById('faucetForm')
  const txForm = document.getElementById('txForm')
  const addressControl = document.getElementById('address')
  const txControl = document.getElementById('tx')
  const clearButton = document.getElementById('clearButton');

  faucetForm.addEventListener('submit', async (e) => {
    e.preventDefault()

    const address = addressControl.value
    const response = await postData('/faucet/request', { address })
    
    txControl.value = response.tx
  })

  
  txForm.addEventListener('submit', async (e) => {
    e.preventDefault()

    const tx = txControl.value
    const response = await postData('/faucet/submit', { tx })

    alert(JSON.stringify(response, null, 2))
  })

  clearButton.addEventListener('click', () => {
    txControl.value = ''
  });
}