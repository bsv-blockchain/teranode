export const getFileKey = (file) => {
  return file.name + '_' + file.lastModified
}
