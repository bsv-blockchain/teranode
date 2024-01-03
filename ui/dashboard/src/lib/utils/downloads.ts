const doDownload = (href, targetFilename) => {
  const link = document.createElement('a')
  link.setAttribute('href', href)
  link.setAttribute('download', targetFilename)
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}

export const downloadFile = (targetFilename, href) => {
  doDownload('data:text/plain;charset=utf-8, ' + encodeURIComponent(href), targetFilename)
}

export const downloadFileBlob = (targetFilename, blob) => {
  const url = window.URL.createObjectURL(new Blob([blob]))
  doDownload(url, targetFilename)
}
