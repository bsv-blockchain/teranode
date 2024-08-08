# State Machine

The mermaid diagram outlined below represents the various states and events that dictate the functionality of the node. To create and visualize the state machine diagram, you can use https://mermaid.live/. This tool allows you to generate the diagram visualization interactively.

```mermaid
stateDiagram-v2
    [*] --> STOPPED
    CATCHINGBLOCKS --> CATCHINGTXS: CATCHUPTXS
    CATCHINGBLOCKS --> MINING: MINE
    CATCHINGBLOCKS --> STOPPED: STOP
    CATCHINGTXS --> MINING: MINE
    CATCHINGTXS --> STOPPED: STOP
    LEGACYSYNCING --> RUNNING: RUN
    MINING --> CATCHINGBLOCKS: CATCHUPBLOCKS
    MINING --> CATCHINGTXS: CATCHUPTXS
    MINING --> STOPPED: STOP
    RESTORING --> RUNNING: RUN
    RUNNING --> MINING: MINE
    RUNNING --> STOPPED: STOP
    STOPPED --> LEGACYSYNCING: LEGACYSYNC
    STOPPED --> RESTORING: RESTORE
    STOPPED --> RUNNING: RUN
```