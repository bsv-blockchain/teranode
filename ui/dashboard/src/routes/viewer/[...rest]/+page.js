export function load ({ params }) {
  const parts = params.rest.split('/')
  if (parts.length > 1) {
    console.log("load", parts)
    return {
      type: parts[0],
      hash: parts[1]
    }
  }

  return {
      type: '',
      hash: ''
  }
}
