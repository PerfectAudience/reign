Overlord
========
A suite of lightweight services for distributed systems coordination and messaging based on Zookeeper and Netty.

Features
--------
Out of the box, Overlord provides the following:
* Service discovery and capacity monitoring
* Distributed locking

Design
------

Part of Overlord's value is in how data is kept in Zookeeper.  By creating a standard layout of information, Overlord allows for an ecosystem that allows non-java services and/or monitoring to easily plug in.

The default layout in Zookeeper is described below.  Custom layouts may be created as necessary by implementing your own PathScheme class.:

###Base paths:

* `/overlord` - the root directory
* `/overlord/internal/_TREE_` - internal to framework
* `/overlord/user/_TREE_` - user-created service data, configuration, locks, etc.


###`_TREE_` is defined as follows:

* `/presence` - service nodes check-in under this path
* `/conf` - service configuration is found under this path
* `/data` - data published by services for consumption by other nodes connected to ZK via Overlord is found here
* `/lock` - data describing distributed locks lives here



