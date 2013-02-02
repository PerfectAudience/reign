overlord
========

Distributed systems coordination and messaging based on Zookeeper and Netty.

Design
------

Part of Overlord's value is in how 

The default path scheme is as follows:

Base path:

/overlord

/overlord/internal/<TREE>

/overlord/user/<TREE>


<TREE> is defined as follows:

/presence
/conf
/data
/lock



