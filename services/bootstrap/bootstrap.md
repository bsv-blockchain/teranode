# 🌐 Bootstrap Service

## Index [TODO ]


## Description [TODO ]

The Bootstrap Service, helps new nodes find peers in a UBSV network. It allows nodes to register themselves and be notified about other nodes' presence, serving as a discovery service.

The service manages a set of subscribers and broadcast notifications and keep-alives to them, to keep them updated about network participants.

** TODO ** Miro Diagram of sorts


- Architecture


    -Jake's Code breakdown     - Process flows / diagrams .
        - Sequence diagrams (1 or more)

```plantuml
@startuml
actor Client
participant "Server" as S
database "Config" as C
database "Blockchain Network" as B
participant "Echo HTTP Server" as E
participant "GRPC Server" as G
participant "WebSocket Client" as W
participant "Subscriber" as Sub

== Initialization ==
Client -> S : Init()
activate S
S -> C : Read Config\n(grpcListenAddress, httpListenAddress)
activate C
C --> S : Config Data
deactivate C
S -> S : Setup Discovery Channel
S -> E : Setup Echo Server\n(HTTP/HTTPS Endpoints)
activate E
E --> S : Echo Server Ready
deactivate E
S -> G : Setup GRPC Server
activate G
G --> S : GRPC Server Ready
deactivate G
S --> Client : Initialized
deactivate S

== Connection Handling ==
Client -> S : Connect(info)
activate S
S -> Sub : Add New Subscriber\n(info)
activate Sub
Sub --> S : Subscriber Added
deactivate Sub
S --> Client : Connected
deactivate S

== Broadcast Notification ==
Client -> S : BroadcastNotification(notification)
activate S
S -> S : Check Subscribers
loop for each Subscriber
    S -> Sub : Send Notification
    activate Sub
    Sub --> S : Acknowledgment
    deactivate Sub
end
S --> Client : Notifications Sent
deactivate S

== Get Nodes ==
Client -> S : GetNodes()
activate S
S -> S : Collect Nodes Information
S --> Client : NodeList
deactivate S

@enduml

```

- input  / output

## Technology and specific Stores

## How to run

###     Preconditions

###     How to run


###     Configuration options (settings flags)


## Troubleshooting



## References (like third party)
