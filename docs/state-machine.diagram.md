# State Machine

The mermaid diagram outlined below represents the various states and events that dictate the functionality of the node. To create and visualize the state machine diagram, you can use https://mermaid.live/. This tool allows you to generate the diagram visualization interactively.

```mermaid
stateDiagram-v2
    [*] --> STOPPED
    CATCHINGBLOCKS --> CATCHINGTXS: CATCHUPTXS
    CATCHINGBLOCKS --> RUNNING: RUN
    CATCHINGBLOCKS --> STOPPED: STOP
    CATCHINGTXS --> RUNNING: RUN
    CATCHINGTXS --> STOPPED: STOP
    LEGACYSYNCING --> RUNNING: RUN
    RESTORING --> RUNNING: RUN
    RUNNING --> CATCHINGBLOCKS: CATCHUPBLOCKS
    RUNNING --> CATCHINGTXS: CATCHUPTXS
    RUNNING --> STOPPED: STOP
    STOPPED --> LEGACYSYNCING: LEGACYSYNC
    STOPPED --> RESTORING: RESTORE
    STOPPED --> RUNNING: RUN
```