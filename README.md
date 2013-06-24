Reign Framework
===============
A suite of lightweight services for distributed systems coordination and messaging based on Zookeeper, Netty, and Web Sockets.


Features
--------

Out of the box, the framework provides the following:
* Service presence - monitor for nodes coming up and going down in services.
* Messaging - nodes can can message each other directly and/or broadcast a message to member nodes of a specific service.
* Distributed locking - support for read/write locks, exclusive locks, semaphores, and barriers (coming soon).
* Reliable Zookeeper client wrapper that handles common ZooKeeper connection/session errors and re-connects as necessary.
* A standardized way of organizing information in ZooKeeper.

Example applications:
* Zero configuration applications - deploy to different environments or change application properties without needing to edit configuration files or restart services. 
* Dynamic service discovery - nodes in one service can discover nodes in other services without configuration changes. 
* Service redundancy - for services where only one process/node can run at the same time, a stand-by process/node can be brought up and will automatically take over if the currently running process/node fails.
* Capacity monitoring - services can monitor each other and ensure that they do not overwhelm each other:  for example, a frontline service may slow down its rate of requests to a backend service to prevent a \"domino effect\" where a spike in traffic brings down the whole application. 
* Application decisioning based on service state - services can publish diagnostic metrics which can be used to change application behavior:  for example, nodes in one service may go into \"safety mode\" based on information provided by another service (error rates, etc.). 

Building on top of Reign quickly provides a high level of cluster-awareness and coordination capabilities:  allowing you to focus your efforts on application functionality, not low level infrastructure.

Reign features a pluggable programming API which allows additional services to be built on top of or in addition to the core services.


Quick Start
-----------






Design Notes
------------

Part of Reign's value is derived from how data is organized in ZooKeeper.  By creating a standard layout of information, the framework sets up the possibility of an ecosystem that allows non-Java services/applications to easily coordinate/monitor each other in a standard fashion.  Monitoring and administration is also made simpler, as the framework's \"magic\" is made more transparent for easier debugging:  for example, in the case of locks, one can even force a node to release a lock by deleting its lock node just using the standard Zookeeper shell!

The default data layout in ZooKeeper is outlined below.  Custom layouts may be created as necessary by implementing your own or customizing the provided `PathScheme` implementation.

###Base paths:

* `/reign` - the root directory
* `/reign/_TREE_` - user-created service data, configuration, locks, etc.


###`_TREE_` is defined as follows:

* `/presence` - service nodes check-in under this path
* `/conf` - configuration data for services, etc. are found under this path
* `/coord` - data describing distributed locks, semaphores, etc. lives here


Web Sockets Protocol
--------------------

By default, services in the framework can receive and response to messages via Web Sockets.

###Message Format
`[TARGET_SERVICE]:[RESOURCE]#[META_COMMAND]`

###Example Messages
`presence:/my_cluster/foo_service` - this message would get information on the `foo_service`.  More information is available in the Web UI available on any node running the framework at port 33033 (default port).


Upcoming
--------

* SASL support for ZooKeeper
* Distributed barriers
* Enhanced administration UI
* Binary protocol



