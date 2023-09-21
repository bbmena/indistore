###This is a work in progress.

A distributed key/value store currently storing data only in-memory. 

###Orchestrator
This is a server that acts as a gateway to the storage nodes, and also stores the DHT tracking which node holds a given value
Receives requests from the client and forwards them on to a node, returning the value to the client upon response

###Node
A storage node. Is assigned a place on the hash ring by the orchestrator and sent values to store accordingly 

###Client
A simple client library to be used within an application to communicate with the Orchestrator