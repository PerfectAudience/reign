package org.kompany.sovereign;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kompany.sovereign.conf.ConfService;
import org.kompany.sovereign.coord.CoordinationService;
import org.kompany.sovereign.data.DataService;
import org.kompany.sovereign.messaging.MessagingProvider;
import org.kompany.sovereign.messaging.MessagingService;
import org.kompany.sovereign.messaging.websocket.WebSocketMessagingProvider;
import org.kompany.sovereign.presence.PresenceService;
import org.kompany.sovereign.util.PathCache;
import org.kompany.sovereign.util.SimplePathCache;
import org.kompany.sovereign.zookeeper.ResilientZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenient way to quickly configure a Sovereign object.
 * 
 * @author ypai
 * 
 */
public class SovereignBuilder {

    private static final Logger logger = LoggerFactory.getLogger(SovereignBuilder.class);

    private String zkConnectString;
    private int zkSessionTimeout = -1;

    private int pathCacheMaxSize = 1024;
    private int pathCacheMaxConcurrencyLevel = 2;

    private PathCache pathCache = null;
    private ZkClient zkClient = null;

    private PathScheme pathScheme = null;

    // private MessagingProvider messagingProvider = null;

    // private final boolean messagingOff = false;

    private final Map<String, Service> serviceMap = new HashMap<String, Service>();

    public SovereignBuilder allCoreServices() {
        PresenceService presenceService = new PresenceService();
        serviceMap.put("presence", presenceService);

        ConfService confService = new ConfService();
        serviceMap.put("conf", confService);

        CoordinationService coordService = new CoordinationService();
        serviceMap.put("coord", coordService);

        DataService dataService = new DataService();
        serviceMap.put("data", dataService);

        MessagingService messagingService = new MessagingService();
        serviceMap.put("messaging", messagingService);

        return this;
    }

    public SovereignBuilder zkClient(String zkConnectString, int zkSessionTimeout) {
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;
        return this;

    }

    public SovereignBuilder registerService(String serviceName, Service service) {
        serviceMap.put(serviceName, service);
        return this;
    }

    public SovereignBuilder pathCache(int maxSize, int concurrencyLevel) {
        this.pathCacheMaxSize = maxSize;
        this.pathCacheMaxConcurrencyLevel = concurrencyLevel;
        return this;
    }

    public SovereignBuilder pathCache(PathCache pathCache) {
        this.pathCache = pathCache;
        return this;
    }

    public SovereignBuilder zkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
        return this;
    }

    // public SovereignBuilder messagingProvider(MessagingProvider messagingProvider) {
    // this.messagingProvider = messagingProvider;
    // return this;
    // }
    //
    // public SovereignBuilder messagingOff(boolean messagingOff) {
    // this.messagingOff = messagingOff;
    // if (this.messagingOff) {
    // this.messagingProvider = null;
    // }
    // return this;
    // }

    public Sovereign build() {

        Sovereign s = null;
        if (zkClient == null) {
            zkClient = defaultZkClient();
        }
        if (pathCache == null) {
            pathCache = defaultPathCache();
        }
        if (pathScheme == null) {
            pathScheme = defaultPathScheme();
        }

        // if (!messagingOff && messagingProvider == null) {
        // messagingProvider = defaultMessagingProvider(Sovereign.DEFAULT_MESSAGING_PORT);
        // }
        s = new Sovereign(zkClient, pathScheme, pathCache);
        // s.setMessagingProvider(!messagingOff ? messagingProvider : null);
        s.registerServices(serviceMap);
        return s;
    }

    ZkClient defaultZkClient() {
        if (zkConnectString == null || zkSessionTimeout <= 0) {
            throw new IllegalStateException("zkConnectString and zkSessionTimeout not configured!");
        }

        ZkClient zkClient = null;
        try {
            zkClient = new ResilientZooKeeper(zkConnectString, zkSessionTimeout);
        } catch (IOException e) {
            throw new IllegalStateException("Fatal error:  could not initialize Zookeeper client!");
        }
        return zkClient;
    }

    PathCache defaultPathCache() {
        if (pathCacheMaxSize < 1 || pathCacheMaxConcurrencyLevel < 1 || zkClient == null) {
            throw new IllegalStateException(
                    "zkClient, pathCacheMaxSize, and pathCacheMaxConcurrencyLevel must be configured to create default path cache!");
        }

        return new SimplePathCache(this.pathCacheMaxSize, this.pathCacheMaxConcurrencyLevel, zkClient);
    }

    PathScheme defaultPathScheme() {
        return new DefaultPathScheme("/sovereign/user", "/sovereign/internal");
    }

    MessagingProvider defaultMessagingProvider(int port) {
        MessagingProvider messagingProvider = new WebSocketMessagingProvider();
        messagingProvider.setPort(port);
        return messagingProvider;

    }

}
