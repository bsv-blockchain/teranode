/**
 * Light theme for Teranode dashboard.
 * Mirrors the dark theme structure with proper light-mode colours.
 */

export const light = {
  easing: {
    function: 'ease-in-out',
    duration: '0.2s',
  },
  focus: {
    rect: {
      color: '#0066CC',
      width: '2px',
      border: {
        radius: '4px',
      },
      padding: '0',
      bg: {
        color: '#0066CC',
      },
    },
  },
  app: {
    box: {
      sizing: 'border-box',
    },
    font: {
      family: 'Satoshi',
    },
    mono: {
      font: {
        family: 'JetBrains Mono',
      },
    },
    bg: {
      color: '#F0F2F5',
    },
    color: '#1C1E21',
    cover: {
      bg: {
        color: 'rgba(240, 242, 245, 0.85)',
      },
    },
    border: {
      color: 'rgba(0, 0, 0, 0.12)',
    },
    subtle: {
      bg: {
        color: 'rgba(0, 0, 0, 0.06)',
      },
    },
    overlay: {
      color: 'rgba(0, 0, 0, 0.06)',
      strong: {
        color: 'rgba(0, 0, 0, 0.10)',
      },
    },
  },
  comp: {
    bg: {
      color: '#FFFFFF',
    },
    color: '#1C1E21',
    font: {
      family: 'Satoshi',
      weight: 400,
    },
    outline: 'none',
    focus: {
      rect: {
        color: '#0066CC',
        width: '2px',
        border: {
          radius: '4px',
        },
        padding: '0',
        bg: {
          color: '#0066CC',
        },
      },
    },
    label: {
      gap: '6px',
      color: 'rgba(28, 30, 33, 0.66)',
      disabled: {
        color: 'rgba(28, 30, 33, 0.4)',
      },
    },
    primary: {
      enabled: {
        color: '#1C1E21',
        bg: { color: '#E4E6EB' },
        border: { color: '#E4E6EB' },
      },
      hover: {
        color: '#1C1E21',
        bg: { color: '#D8DADF' },
        border: { color: '#D8DADF' },
      },
      active: {
        color: '#1C1E21',
        bg: { color: '#BEC3CC' },
        border: { color: '#BEC3CC' },
      },
      focus: {
        color: '#1C1E21',
        bg: { color: '#BEC3CC' },
        border: { color: '#BEC3CC' },
      },
      disabled: {
        color: '#8A8D91',
        bg: { color: '#F0F2F5' },
        border: { color: '#F0F2F5' },
      },
    },
    secondary: {
      enabled: {
        color: '#FFFFFF',
        bg: { color: '#ce1722' },
        border: { color: '#ce1722' },
      },
      hover: {
        color: '#FFFFFF',
        bg: { color: '#b0141e' },
        border: { color: '#b0141e' },
      },
      active: {
        color: '#FFFFFF',
        bg: { color: '#96030d' },
        border: { color: '#96030d' },
      },
      focus: {
        color: '#FFFFFF',
        bg: { color: '#96030d' },
        border: { color: '#96030d' },
      },
      disabled: {
        color: '#8A8D91',
        bg: { color: '#D8DADF' },
        border: { color: '#D8DADF' },
      },
    },
    tertiary: {
      enabled: {
        color: '#FFFFFF',
        bg: { color: '#0866FF' },
        border: { color: '#0866FF' },
      },
      hover: {
        color: '#FFFFFF',
        bg: { color: '#0550d1' },
        border: { color: '#0550d1' },
      },
      active: {
        color: '#FFFFFF',
        bg: { color: '#0039a6' },
        border: { color: '#0039a6' },
      },
      focus: {
        color: '#FFFFFF',
        bg: { color: '#0039a6' },
        border: { color: '#0039a6' },
      },
      disabled: {
        color: '#8A8D91',
        bg: { color: '#C4D4F5' },
        border: { color: '#C4D4F5' },
      },
    },
    destructive: {
      enabled: {
        color: '#FFFFFF',
        bg: { color: '#ce1722' },
        border: { color: '#ce1722' },
      },
      hover: {
        color: '#FFFFFF',
        bg: { color: '#b0141e' },
        border: { color: '#b0141e' },
      },
      active: {
        color: '#FFFFFF',
        bg: { color: '#96030d' },
        border: { color: '#96030d' },
      },
      focus: {
        color: '#FFFFFF',
        bg: { color: '#96030d' },
        border: { color: '#96030d' },
      },
      disabled: {
        color: '#8A8D91',
        bg: { color: '#F5C6CA' },
        border: { color: '#F5C6CA' },
      },
    },
    tool: {
      enabled: {
        color: '#1C1E21',
        bg: { color: 'transparent' },
        border: { color: 'transparent' },
      },
      hover: {
        color: '#1C1E21',
        bg: { color: 'rgba(0, 0, 0, 0.08)' },
        border: { color: 'transparent' },
      },
      active: {
        color: '#FFFFFF',
        bg: { color: 'rgba(0, 0, 0, 0.72)' },
        border: { color: 'transparent' },
      },
      focus: {
        color: '#FFFFFF',
        bg: { color: 'rgba(0, 0, 0, 0.72)' },
        border: { color: 'transparent' },
      },
      disabled: {
        color: '#8A8D91',
        bg: { color: '#F0F2F5' },
        border: { color: 'transparent' },
      },
    },
  },
  json: {
    display: {
      color: '#374151',
      key: { color: '#1d4ed8' },
      string: { color: '#166534' },
      number: { color: '#92400e' },
      boolean: { color: '#0e7490' },
      null: { color: '#6b7280' },
    },
    tree: {
      string: { color: '#166534' },
      string2: { color: '#6b00c2' },
      boolean: { color: '#1255a0' },
    },
  },
  footer: {
    height: '60px',
    bg: { color: 'transparent' },
    border: { color: 'rgba(0, 0, 0, 0.08)' },
    link: {
      color: 'rgba(28, 30, 33, 0.66)',
      hover: { color: 'rgba(28, 30, 33, 0.88)' },
    },
  },
  input: {
    placeholder: {
      color: '#8A8D91',
    },
    default: {
      enabled: {
        color: '#1C1E21',
        bg: { color: '#FFFFFF' },
        border: { color: 'rgba(0, 0, 0, 0.2)' },
      },
      hover: {
        color: '#1C1E21',
        bg: { color: '#FFFFFF' },
        border: { color: 'rgba(0, 0, 0, 0.4)' },
      },
      active: {
        color: '#1C1E21',
        bg: { color: '#FFFFFF' },
        border: { color: 'rgba(0, 0, 0, 0.4)' },
      },
      focus: {
        color: '#1C1E21',
        bg: { color: '#FFFFFF' },
        border: { color: '#0866FF' },
      },
      disabled: {
        color: 'rgba(28, 30, 33, 0.4)',
        bg: { color: '#F0F2F5' },
        border: { color: 'rgba(0, 0, 0, 0.1)' },
      },
      invalid: {
        border: { color: '#E02020' },
      },
    },
  },
  dropdown: {
    list: {
      bg: { color: '#FFFFFF' },
      border: {
        radius: '8px',
        color: 'rgba(0, 0, 0, 0.15)',
      },
      padding: '8px',
      item: {
        enabled: { bg: { color: 'rgba(0, 0, 0, 0)' } },
        hover: { bg: { color: 'rgba(0, 0, 0, 0.05)' } },
        selected: { bg: { color: 'rgba(0, 0, 0, 0.1)' } },
      },
    },
  },
  table: {
    bg: { color: 'var(--app-bg-color)' },
    border: {
      top: { left: { radius: '12px' }, right: { radius: '12px' } },
      bottom: { left: { radius: '0' }, right: { radius: '0' } },
    },
    th: {
      bg: { color: '#E4E6EB' },
      color: '#444950',
      small: {
        color: '#8A8D91',
        font: { weight: 400 },
      },
      text: { transform: 'none' },
      padding: '9px 24px',
      height: 'unset',
      border: {
        top: { left: { radius: '12px' }, right: { radius: '12px' } },
        bottom: { left: { radius: '12px' }, right: { radius: '12px' } },
      },
      font: { size: '12px', weight: 700 },
      line: { height: '18px' },
      letter: { spacing: '0.26px' },
    },
    td: {
      color: 'rgba(28, 30, 33, 0.88)',
      padding: '0 24px',
      border: {
        top: 'none',
        bottom: '1px solid rgba(0, 0, 0, 0.08)',
      },
      font: { size: '14px', weight: 400 },
      line: { height: '24px' },
      letter: { spacing: '0.3px' },
    },
    tr: {
      first: { child: { td: { border: { top: 'none' } } } },
      last: { child: { td: { border: { bottom: 'none' } } } },
    },
  },
  link: {
    default: {
      enabled: { color: '#0866FF' },
      hover: { color: '#0550d1' },
      active: { color: '#0039a6' },
      bold: {
        color: '#0866FF',
        text: { decoration: { line: 'underline' } },
      },
      visited: { color: '#5B189B' },
    },
  },
  toast: {
    // bar fallback defined in GlobalStyle; here we just ensure it's visible
    bar: { bg: { color: 'rgba(0, 0, 0, 0.15)' } },
    success: {
      bg: { color: '#FFFFFF' },
      border: { color: '#00AB01' },
    },
    failure: {
      bg: { color: '#FFFFFF' },
      border: { color: '#E8001C' },
    },
    warn: {
      bg: { color: '#FFFFFF' },
      border: { color: '#FFC700' },
    },
    info: {
      bg: { color: '#FFFFFF' },
      border: { color: '#0094FF' },
    },
  },
  msgbox: {
    bg: { color: '#FFFFFF' },
    label: { color: '#606770' },
    value: { color: '#1C1E21' },
    block: { border: { color: '#0866FF' } },
    mining_on: { border: { color: '#00897B' } },
    subtree: { border: { color: '#6148C4' } },
    ping: { border: { color: '#BDBDBD' } },
    getminingcandidate: { border: { color: '#2778FF' } },
    node_status: { border: { color: '#FF9500' } },
    default: { border: { color: '#FF9500' } },
  },
  switch: {
    default: {
      enabled: {
        bg: { color: '#BEC3CC' },
        thumb: { color: '#FFFFFF' },
        border: { color: '#BEC3CC' },
      },
      checked: {
        bg: { color: '#0866FF' },
        thumb: { color: '#FFFFFF' },
        border: { color: '#0866FF' },
      },
      disabled: {
        bg: { color: '#E4E6EB' },
        thumb: { color: '#FFFFFF' },
        border: { color: '#E4E6EB' },
      },
    },
  },
  toggle: {
    bg: {
      color: '#E4E6EB',
    },
  },
  tab: {
    default: {
      enabled: {
        color: '#606770',
        bg: { color: 'transparent' },
        border: { color: 'transparent' },
      },
      hover: {
        color: '#1C1E21',
        bg: { color: '#E4E6EB' },
        border: { color: 'transparent' },
      },
      active: {
        color: '#1C1E21',
        bg: { color: '#F2B33B' },
        border: { color: 'transparent' },
      },
      focus: {
        color: '#606770',
        bg: { color: 'transparent' },
        border: { color: 'transparent' },
      },
      selected: {
        color: '#1C1E21',
        bg: { color: 'transparent' },
        border: { color: '#1C1E21' },
      },
      disabled: {
        color: '#BEC3CC',
        bg: { color: 'transparent' },
        border: { color: 'transparent' },
      },
    },
  },
}
