# State Machine

The mermaid diagram outlined below represents the various states and events that dictate the functionality of the node. To create and visualize the state machine diagram, you can use https://mermaid.live/. This tool allows you to generate the diagram visualization interactively.

```mermaid
stateDiagram-v2
    [*] --> STOPPED
    CATCHINGBLOCKS --> CATCHINGLEGACY: CATCHUPLEGACY
    CATCHINGBLOCKS --> CATCHINGTXS: CATCHUPTXS
    CATCHINGBLOCKS --> MINING: MINE
    CATCHINGBLOCKS --> STOPPED: STOP
    CATCHINGLEGACY --> CATCHINGBLOCKS: CATCHUPBLOCKS
    CATCHINGLEGACY --> CATCHINGTXS: CATCHUPTXS
    CATCHINGLEGACY --> MINING: MINE
    CATCHINGLEGACY --> RUNNING: RUN
    CATCHINGLEGACY --> STOPPED: STOP
    CATCHINGTXS --> CATCHINGLEGACY: CATCHUPLEGACY
    CATCHINGTXS --> MINING: MINE
    CATCHINGTXS --> STOPPED: STOP
    MINING --> CATCHINGBLOCKS: CATCHUPBLOCKS
    MINING --> CATCHINGLEGACY: CATCHUPLEGACY
    MINING --> CATCHINGTXS: CATCHUPTXS
    MINING --> STOPPED: STOP
    RUNNING --> CATCHINGLEGACY: CATCHUPLEGACY
    RUNNING --> MINING: MINE
    RUNNING --> STOPPED: STOP
    STOPPED --> RUNNING: RUN
```