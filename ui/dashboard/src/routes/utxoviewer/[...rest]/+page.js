export function load({ params }) {
  const parts = params.rest.split('/')

  const hash = parts[0]

  return {
    hash,
  }
}
