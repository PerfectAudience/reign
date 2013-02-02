Overlord
========
A suite of lightweight services for distributed systems coordination and messaging based on Zookeeper and Netty.


Features
--------
Overlord features a pluggable programming API which allows additional functionality to be easily.

Out of the box, Overlord provides the following:
* Service discovery - monitor for nodes coming up and going down in services.
* Service data publishing - services can publish information for consumption by other services.
* Distributed locking - support for read/write locks, exclusive locks, and semaphores.
* Reliable Zookeeper client wrapper that handles common Zookeeper errors and re-connects as necessary.


Easy to Use
-----------




Design Notes
------------

Part of Overlord's value is in how data is kept in Zookeeper.  By creating a standard layout of information, Overlord sets up the possibility of an ecosystem that allows non-Java services/applications to easily connect and coordinate via Zookeeper.

The default layout in Zookeeper is described below.  Custom layouts may be created as necessary by implementing your own `PathScheme` class.:

###Base paths:

* `/overlord` - the root directory
* `/overlord/internal/_TREE_` - internal to framework
* `/overlord/user/_TREE_` - user-created service data, configuration, locks, etc.


###`_TREE_` is defined as follows:

* `/presence` - service nodes check-in under this path
* `/conf` - configuration data for services, etc. are found under this path
* `/data` - data published by services for consumption by other nodes connected to ZK via Overlord is found here
* `/lock` - data describing distributed locks lives here



