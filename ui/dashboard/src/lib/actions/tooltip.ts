import tippy from 'tippy.js'

// Track the currently visible tooltip globally
let currentTooltip: any = null

export const tooltip = (node, params: any = {}) => {
  if (!params.content) {
    return null
  }
  // Determine the title to show. We want to prefer
  //    the custom content passed in first, then the
  // HTML title attribute then the aria-label
  // in that order.
  const custom = params.content
  const title = node.title
  const label = node.getAttribute('aria-label')
  const content = custom || title || label

  // Let's make sure the "aria-label" attribute
  // is set so our element is accessible:
  // https://developer.mozilla.org/en-US/docs/Web/Accessibility/ARIA/ARIA_Techniques/Using_the_aria-label_attribute
  if (!label) node.setAttribute('aria-label', content)

  // Clear out the HTML title attribute since
  // we don't want the default behavior of it
  // showing up on hover.
  node.title = ''

  // Add onShow handler to hide previous tooltip
  const enhancedParams = {
    ...params,
    content,
    onShow(instance) {
      // Hide the currently visible tooltip if it's different
      if (currentTooltip && currentTooltip !== instance) {
        currentTooltip.hide()
      }
      currentTooltip = instance
      
      // Call original onShow if provided
      if (params.onShow) {
        params.onShow(instance)
      }
    },
    onHidden(instance) {
      // Clear reference if this was the current tooltip
      if (currentTooltip === instance) {
        currentTooltip = null
      }
      
      // Call original onHidden if provided
      if (params.onHidden) {
        params.onHidden(instance)
      }
    },
  }

  // Support any of the Tippy props by forwarding all "params":
  // https://atomiks.github.io/tippyjs/v6/all-props/
  const tip: any = tippy(node, enhancedParams)

  return {
    // If the props change, let's update the Tippy instance:
    update: (newParams) => {
      const updatedParams = {
        ...newParams,
        content,
        onShow(instance) {
          if (currentTooltip && currentTooltip !== instance) {
            currentTooltip.hide()
          }
          currentTooltip = instance
          if (newParams.onShow) {
            newParams.onShow(instance)
          }
        },
        onHidden(instance) {
          if (currentTooltip === instance) {
            currentTooltip = null
          }
          if (newParams.onHidden) {
            newParams.onHidden(instance)
          }
        },
      }
      tip.setProps(updatedParams)
    },

    // Clean up the Tippy instance on unmount:
    destroy: () => {
      if (currentTooltip === tip) {
        currentTooltip = null
      }
      tip.destroy()
    },
  }
}

export const createTippy = (defaultProps) => (element, props) =>
  tooltip(element, { ...defaultProps, ...props })
