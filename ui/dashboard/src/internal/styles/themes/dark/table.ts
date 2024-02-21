export const table = {
  bg: {
    color: 'var(--app-bg-color)',
  },
  border: {
    top: {
      left: {
        radius: '12px',
      },
      right: {
        radius: '12px',
      },
    },
    bottom: {
      left: {
        radius: '0',
      },
      right: {
        radius: '0',
      },
    },
  },
  th: {
    bg: {
      color: '#1F2328',
    },
    color: '#B3B4B7',
    small: {
      color: '#898C90',
      font: {
        weight: 400,
      },
    },
    text: {
      transform: 'none',
    },
    padding: '9px 24px',
    height: 'unset',
    border: {
      top: {
        left: {
          radius: '12px',
        },
        right: {
          radius: '12px',
        },
      },
      bottom: {
        left: {
          radius: '12px',
        },
        right: {
          radius: '12px',
        },
      },
    },
    font: {
      size: '13px',
      weight: 700,
    },
    line: {
      height: '18px',
    },
    letter: {
      spacing: '0.26px',
    },
  },
  td: {
    color: 'rgba(255, 255, 255, 0.88)',
    padding: '0 24px',
    border: {
      top: 'none',
      bottom: '1px solid rgba(255, 255, 255, 0.08)',
    },
    font: {
      size: '15px',
      weight: 400,
    },
    line: {
      height: '24px',
    },
    letter: {
      spacing: '0.3px',
    },
  },
  tr: {
    first: {
      child: {
        td: {
          border: {
            top: 'none',
          },
        },
      },
    },
    last: {
      child: {
        td: {
          border: {
            bottom: 'none',
          },
        },
      },
    },
  },
}
