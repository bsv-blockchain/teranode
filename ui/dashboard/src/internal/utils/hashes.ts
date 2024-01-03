export const reverseHash = (hash: string) => {
  if (hash && hash.length === 64) {
    const hexString = hash
    const byteLength = hexString.length / 2
    const byteArray = new Uint8Array(byteLength)

    for (let i = 0; i < byteLength; i++) {
      const byte = parseInt(hexString.substr(i * 2, 2), 16)
      byteArray[i] = byte
    }

    byteArray.reverse()

    let reversedHexString = ''
    for (let i = 0; i < byteArray.length; i++) {
      reversedHexString += byteArray[i].toString(16).padStart(2, '0')
    }
    return reversedHexString
  }
  return hash
}
