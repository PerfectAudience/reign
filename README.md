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

Common use cases:
* Zero configuration applications - deploy to different environments or change application properties without needing to edit configuration files or restart services.  Edit configuration in one place and push changes out to many nodes at once. 
* Dynamic service discovery - nodes in one service can discover nodes in other services without configuration changes. 
* Service redundancy - for services where only one process/node can run at the same time, a stand-by process/node can be brought up and will automatically take over if the currently running process/node fails.
* Capacity monitoring - services can monitor each other and ensure that they do not overwhelm each other:  for example, a frontline service may slow down its rate of requests to a backend service to prevent a "domino effect" where a spike in traffic brings down the whole application. 
* Application decisioning based on service state - services can publish diagnostic metrics which can be used to change application behavior:  for example, nodes in one service may go into "safety mode" based on information provided by another service (error rates, etc.). 

Application using Reign quickly gain a high level of cluster-awareness and coordination capabilities.  In addition, Reign provides a programming API which allows additional capabilities to be built on top of or in addition to the core services.  Distributed services using Reign can easily communicate with each other by sending messages to a specific node or broadcasting to an entire service.




Quick Start
-----------
The following code and a simple Java `main()` hook can be found in  
`io.reign.examples.QuickStartExample`

### Prerequisites

Have a running ZooKeeper cluster.  For a quick guide on how to set up ZooKeeper on OS X, try 
http://blog.kompany.org/2013/02/23/setting-up-apache-zookeeper-on-os-x-in-five-minutes-or-less/



### Initialize and start up examples
        /**
         * init and start with core services -- connecting to ZooKeeper on localhost at port 2181 with 30 second
         * ZooKeeper session timeout
         **/
        Reign reign = Reign.maker().core("localhost:2181", 30000).get();
        reign.start();
        
        /**
         * init and start with core services -- connecting to a ZooKeeper cluster at port 2181 with 30 second
         * ZooKeeper session timeout
         **/
        Reign reign = Reign.maker().core("zk-host1:2181,zk-host2:2181,zk-host3:2181", 30000).get();
        reign.start();      
        
        /**
         * init and start with core services -- connecting to a ZooKeeper cluster at port 2181 with 30 second
         * ZooKeeper session timeout using a custom root path, effectively "chroot-ing" the ZooKeeper session:  
         * this is one way to share a ZooKeeper cluster without worrying about path collision  
         **/
        Reign reign = Reign.maker().core("zk-host1:2181,zk-host2:2181,zk-host3:2181/custom_root_path", 30000).get();
        reign.start();           

### Equivalent configuration using Spring

##### Example Spring Bean XML
    <!-- Reign bean configuration -->
    <bean id="reignMaker" class="io.reign.util.spring.SpringReignMaker"  
        init-method="initialize"  
        destroy-method="destroy">
        <property name="zkConnectString" value="localhost:2181"/>
        <property name="zkSessionTimeout" value="30000"/>
        <property name="core" value="true"/>
    </bean>
        
##### Usage in Java code...
    // get and start Reign object
    SpringReignMaker springReignMaker.get = ...injected dependency...;
    Reign reign = springReignMaker.get();
    
    // may not have to do this if bean init-method is specified as "initializeAndStart"
    // in Spring configuration
    reign.start();

### Announcing availability of a service on a node

        /** presence service example **/
        // get the presence service
        PresenceService presenceService = reign.getService("presence");

        // announce this node's available for a given service, immediately visible
        presenceService.announce("examples", "service1", true);

        // announce this node's available for another service, not immediately visible
        presenceService.announce("examples", "service2", false);

        // hide service1
        presenceService.hide("examples", "service1");

        // show service2
        presenceService.show("examples", "service2");

### Check out the Web UI
On any node running the framework, the Web UI is available at port 33033 (assuming the default port was not changed).  For example, if you are running the framework locally, point your browser to 
[http://localhost:33033](http://localhost:33033).

Run one of the examples and in the terminal, you should be able to send the following messages and see the corresponding responses (more information is available on the "Terminal Guide" tab):

List available services in cluster namespace "examples":  
`presence:/examples`

List nodes comprising "service1":  
`presence:/examples/service1`

List nodes comprising "service2":  
`presence:/examples/service2`

### Store configuration in ZooKeeper

        /** configuration service example **/
        // get the configuration service
        ConfService confService = (ConfService) reign.getService("conf");

        // store configuration as properties file
        Properties props = new Properties();
        props.setProperty("capacity.min", "111");
        props.setProperty("capacity.max", "999");
        props.setProperty("lastSavedTimestamp", System.currentTimeMillis() + "");
        confService.putConf("examples", "config1.properties", props);

        // retrieve configuration as properties file
        Properties loadedProperties = confService.getConf("examples", "config1.properties");

        // store configuration as JSON file (uses utility buildable Map for conciseness)
        confService.putConf("examples", "config1.js", Structs.<String, String> map().kv("capacity.min", "222").kv("capacity.max", "888")
                        .kv("lastSavedTimestamp", System.currentTimeMillis() + ""));

        // retrieve configuration as JSON file
        Map<String, String> loadedJson = confService.getConf("examples", "config1.js");

### Messaging between nodes

        /** messaging example **/
        // get the messaging service
        MessagingService messagingService = reign.getService("messaging");

        // wait indefinitely for at least one node in "service1" to become available
        presenceService.waitUntilAvailable("examples", "service1", -1);

        // send message to a single node in the "service1" service in the "examples" cluster;
        // in this example, we are just messaging ourselves
        CanonicalId canonicalId = reign.getCanonicalId();
        String canonicalIdString = reign.getPathScheme().toPathToken(canonicalId);
        ResponseMessage responseMessage = messagingService.sendMessage("examples", "service1", canonicalIdString,
                new SimpleRequestMessage("presence", "/"));

        // broadcast a message to all nodes belonging to the "service1" service in the examples cluster
        Map<String, ResponseMessage> responseMap = messagingService.sendMessage("examples", "service1",
                new SimpleRequestMessage("presence", "/examples"));


### Get and use distributed locks

        /** coordination service example **/
        // get the coordination service
        CoordinationService coordService = (CoordinationService) reign.getService("coord");

        // get a distributed reentrant lock and use it
        DistributedReentrantLock lock = coordService.getReentrantLock("examples", "exclusive_lock1");
        lock.lock();
        try {
            // do some stuff here... (just sleeping 5 seconds)
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do something here...

        } finally {
            lock.unlock();
            lock.destroy();
        }

        // get a read/write distributed lock and use it
        DistributedReadWriteLock rwLock = coordService.getReadWriteLock("examples", "rw_lock1");
        rwLock.readLock().lock();
        try {
            // do some stuff here... (just sleeping 5 seconds)
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do something here...

        } finally {
            rwLock.readLock().unlock();
            rwLock.destroy();
        }


### Shutting down 

        /** shutdown reign **/
        reign.stop();



Design Notes
------------

Part of Reign's value is derived from how data is organized in ZooKeeper.  By creating a standard layout of information, the framework sets up the possibility of an ecosystem that allows non-Java services/applications to easily coordinate/monitor each other in a standard fashion.  Monitoring and administration is also made simpler, as the framework's "magic" is made more transparent for easier debugging:  for example, in the case of locks, one can force a node to release a lock by deleting its lock node just using the standard Zookeeper shell -- of course, this may have unintended consequences and should be done with care.

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

By default, services in the framework can receive and respond to messages via Web Sockets.

###Message Format
`[TARGET_SERVICE]:[RESOURCE]#[META_COMMAND]`

###Example Messages
`presence:/my_cluster/foo_service` - this message would get information on the `foo_service`.  More information is available in the Web UI available on any node running the framework at port 33033 (default port).


Upcoming
--------

* SASL support for ZooKeeper
* Distributed barriers
* Ongoing UI Enhancements (suggestions welcome!)
* Binary protocol
* Async messaging API
* Consistent hashing service/feature



