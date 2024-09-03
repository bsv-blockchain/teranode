# State Machine

The mermaid diagram outlined below represents the various states and events that dictate the functionality of the node. To create and visualize the state machine diagram, you can use https://mermaid.live/. This tool allows you to generate the diagram visualization interactively.

```mermaid
stateDiagram-v2
    [*] --> STOPPED
    CATCHINGBLOCKS --> CATCHINGTXS: CATCHUPTXS
    CATCHINGBLOCKS --> STOPPED: STOP
    CATCHINGBLOCKS --> RESOURCE_UNAVAILABLE: UNAVAILABLE
    CATCHINGTXS --> STOPPED: STOP
    CATCHINGTXS --> RESOURCE_UNAVAILABLE: UNAVAILABLE
    LEGACYSYNCING --> RUNNING: RUN
    LEGACYSYNCING --> RESOURCE_UNAVAILABLE: UNAVAILABLE
    RESOURCE_UNAVAILABLE --> STOPPED: STOP
    RESTORING --> RUNNING: RUN
    RESTORING --> RESOURCE_UNAVAILABLE: UNAVAILABLE
    RUNNING --> CATCHINGBLOCKS: CATCHUPBLOCKS
    RUNNING --> CATCHINGTXS: CATCHUPTXS
    RUNNING --> STOPPED: STOP
    RUNNING --> RESOURCE_UNAVAILABLE: UNAVAILABLE
    STOPPED --> LEGACYSYNCING: LEGACYSYNC
    STOPPED --> RESTORING: RESTORE
    STOPPED --> RUNNING: RUN
```