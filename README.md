Reign
=========
A suite of lightweight services for distributed systems coordination and messaging based on Zookeeper, Netty, and Web Sockets.


Features
--------

Out of the box, Reign provides the following:
* Service presence - monitor for nodes coming up and going down in services.
* Service data publishing - services can publish information for consumption by other services.
* Distributed locking - support for read/write locks, exclusive locks, barriers, and semaphores.
* Reliable Zookeeper client wrapper that handles common Zookeeper errors and re-connects as necessary.

Example applications:
* Zero configuration applications - deploy to different environments or change application properties without needing to edit configuration files or restart services. 
* Dynamic service discovery - nodes in one service can discover nodes in other services without configuration changes. 
* Service redundancy - for services where only one process/node can run at the same time, a stand-by process/node can be brought up and will automatically take over if the currently running process/node fails.
* Capacity monitoring - services can monitor each other and ensure that they do not overwhelm each other:  for example, a frontline service may slow down its rate of requests to a backend service to prevent a \"domino effect\" where a spike in traffic brings down the whole application. 
* Application decisioning based on service state - services can publish diagnostic metrics which can be used to change application behavior:  for example, the application may go into \"safety mode\" based on the publish error rates or event counts of one of its services. 
* Task division amongst \"worker\" nodes - worker nodes can divide work amongst themselves without a centralized coordinator. 

Building on top of Reign quickly provides a high level of cluster-awareness and coordination capabilities:  allowing you to focus your efforts on application functionality, not low level infrastructure.

Reign features a pluggable programming API which allows additional services to be built on top of or in addition to the included core services.


Easy to Use
-----------




Design Notes
------------

Part of Sovereign's value is derived from how data is organized in Zookeeper.  By creating a standard layout of information, Sovereign sets up the possibility of an ecosystem that allows non-Java services/applications to easily coordinate via Zookeeper in a standard fashion.  Monitoring and administration is also made simpler, as the framework's \"magic\" is made more transparent for easier debugging:  for example, in the case of locks, one can even force a node to release a lock by deleting its lock node just using the standard Zookeeper shell!

The default layout in Zookeeper is outlined below.  Custom layouts may be created as necessary by implementing your own `PathScheme` class.:

###Base paths:

* `/reign` - the root directory
* `/reign/internal/_TREE_` - data internal to the framework
* `/reign/user/_TREE_` - user-created service data, configuration, locks, etc.


###`_TREE_` is defined as follows:

* `/presence` - service nodes check-in under this path
* `/conf` - configuration data for services, etc. are found under this path
* `/data` - data published by services for consumption by other nodes connected to ZK via the framework is found here
* `/lock` - data describing distributed locks lives here


Web Sockets Protocol
--------------------

By default, services in Sovereign can receive and response to messages via Web Sockets -- this can be overridden with a custom MessagingProvider.

###Message Format
`[SOVEREIGN_SERVICE_ID]:[RESOURCE]#[META_COMMAND]`

###Example Messages
`presence:my_cluster/foo_service#observe` - this message would get information on the `foo_service` and observe the node.  The Web Sockets client would receive a data update should `foo_service` change.


Upcoming
--------

* SASL support



