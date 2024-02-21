export const getBtnData = (
  totalPages,
  selectedPage,
  isLastPage,
  hasBoundaryRight,
  boundaryCount,
  siblingCount,
) => {
  let data: { type: 'page' | 'range'; page?: number; range?: number[] }[] = []

  // calculate page button data
  // first we consider hasBoundaryRight. if false, it means we do not know how many items there are,
  // so we cannot know the total number of buttons in advance. if we get data that is less than pageSize,
  // then we know we are on the last page (dataSize < pageSize).
  // we assume a layout as follows:
  // [1][.]            -- selectedPage: 1
  // [1][2][.]         -- selectedPage: 2
  // [1][2][3][.]      -- selectedPage: 3
  // [1][.][3][4][.]   -- selectedPage: 4
  // [1][.][4][5][.]   -- selectedPage: 5
  // [1][.][5][6]      -- selectedPage: 6 - where dataSize < pageSize (so last page)

  if (!hasBoundaryRight) {
    if (selectedPage <= boundaryCount + siblingCount + 1) {
      // [1][.] - [1][2][3][.]
      data = Array.from(Array(selectedPage).keys()).map((page) => ({
        type: 'page',
        page: page + 1,
      }))
    } else {
      // [1][.][3][4][.]
      data = Array.from(Array(boundaryCount).keys()).map((page) => ({
        type: 'page',
        page: page + 1,
      }))
      data.push({
        type: 'range',
        range: [boundaryCount, selectedPage - siblingCount - 1],
      })
      data = data.concat(
        Array.from(Array(siblingCount + 1).keys()).map((page) => ({
          type: 'page',
          page: selectedPage - siblingCount + page,
        })),
      )
    }
    if (!isLastPage) {
      data.push({
        type: 'range',
        range: [selectedPage + 1, selectedPage + 1],
      })
    }
    return data
  }

  let displayAllButtons = false
  let totalButtons = 2 * boundaryCount + (1 + 2 * siblingCount)
  const delta = totalPages - totalButtons
  if (delta >= 0) {
    totalButtons += Math.min(2, delta)
    if (totalButtons >= totalPages) {
      displayAllButtons = true
    }
  } else {
    displayAllButtons = true
  }

  if (displayAllButtons) {
    data = Array.from(Array(totalPages).keys()).map((page) => ({
      type: 'page',
      page: page + 1,
    }))
  } else {
    //
    // for dynamic layouts we have 3 possible layout configurations based on selectedPage
    // with totalPages = 10, boundaryCount = 2 and siblingCount = 1, we calculate: totalButtons = 9
    // then
    // config 1: (selectedPage <= boundaryCount + siblingCount + 1)
    // selectedPage = 1, 2, 3, 4
    // [1][2][3][4][5][6][.][9][10]
    // config 2:
    // selectedPage = 5 (selectedPage > boundaryCount + siblingCount + 1) && (selectedPage < totalPages - boundaryCount - siblingCount)
    // [1][2][.][4][5][6][.][9][10]
    // selectedPage = 6
    // [1][2][.][5][6][7][.][9][10]
    // config 3: (selectedPage >= totalPages - boundaryCount - siblingCount)
    // selectedPage = 7, 8, 9, 10
    // [1][2][.][5][6][7][8][9][10]

    if (selectedPage <= boundaryCount + siblingCount + 1) {
      // config 1
      data = Array.from(Array(totalButtons - boundaryCount - 1).keys()).map((page) => ({
        type: 'page',
        page: page + 1,
      }))
      data.push({
        type: 'range',
        range: [totalButtons - boundaryCount, totalPages - boundaryCount],
      })
      data = data.concat(
        Array.from(Array(boundaryCount).keys()).map((page) => ({
          type: 'page',
          page: totalPages - boundaryCount + 1 + page,
        })),
      )
    } else if (
      selectedPage > boundaryCount + siblingCount + 1 &&
      selectedPage < totalPages - boundaryCount - siblingCount
    ) {
      // config 2
      data = Array.from(Array(boundaryCount).keys()).map((page) => ({
        type: 'page',
        page: page + 1,
      }))
      data.push({
        type: 'range',
        range: [boundaryCount + 1, selectedPage - siblingCount - 1],
      })
      data = data.concat(
        Array.from(Array(1 + 2 * siblingCount).keys()).map((page) => ({
          type: 'page',
          page: selectedPage - siblingCount + page,
        })),
      )
      data.push({
        type: 'range',
        range: [selectedPage + siblingCount + 1, totalPages - boundaryCount],
      })
      data = data.concat(
        Array.from(Array(boundaryCount).keys()).map((page) => ({
          type: 'page',
          page: totalPages - boundaryCount + 1 + page,
        })),
      )
    } else {
      // config 3
      data = Array.from(Array(boundaryCount).keys()).map((page) => ({
        type: 'page',
        page: page + 1,
      }))
      data.push({
        type: 'range',
        range: [boundaryCount + 1, totalPages - (totalButtons - boundaryCount - 1)],
      })
      data = data.concat(
        Array.from(Array(totalButtons - boundaryCount - 1).keys()).map((page) => ({
          type: 'page',
          page: totalPages - (totalButtons - boundaryCount - 2) + page,
        })),
      )
    }
  }

  return data
}

const getPageSizeLabel = (t, baseKey, rows) => {
  if (rows === -1) {
    return t ? t(`${baseKey}.all_rows`) : `All`
  } else {
    return t ? t(`${baseKey}.rows`, { rows }) : `${rows} Rows`
  }
}

export const getPageSizeOptions = (i18n) => {
  const { t, baseKey } = i18n
  return [
    {
      label: getPageSizeLabel(t, baseKey, 5),
      value: 5,
    },
    {
      label: getPageSizeLabel(t, baseKey, 10),
      value: 10,
    },
    {
      label: getPageSizeLabel(t, baseKey, 20),
      value: 20,
    },
    {
      label: getPageSizeLabel(t, baseKey, 50),
      value: 50,
    },
    {
      label: getPageSizeLabel(t, baseKey, 100),
      value: 100,
    },
    // {
    //   label: getPageSizeLabel(t, baseKey, -1),
    //   value: -1,
    // },
  ]
}
