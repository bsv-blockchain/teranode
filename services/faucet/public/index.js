async function postData (url = '', data = {}) {
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(data)
  })

  return response.json()
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
    const response = await postData('/api/faucet', { address })
    
    txControl.value = response.tx
  })

  
  txForm.addEventListener('submit', async (e) => {
    e.preventDefault()

    const tx = txControl.value
    const response = await postData('/api/submit', { tx })

    alert(JSON.stringify(response, null, 2))
  })

  clearButton.addEventListener('click', () => {
    txControl.value = ''
  });
}