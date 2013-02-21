package org.kompany.overlord;

import java.util.HashMap;
import java.util.Map;

import org.kompany.overlord.conf.ConfService;
import org.kompany.overlord.coord.CoordinationService;
import org.kompany.overlord.presence.PresenceService;

/**
 * Convenient way to quickly configure a Sovereign object.
 * 
 * @author ypai
 * 
 */
public class SovereignBuilder {

    private String zkConnectString;
    private int zkSessionTimeout;

    private int pathCacheSize = 1024;
    private int pathCacheConcurrencyLevel = 8;

    private final Map<String, Service> serviceMap = new HashMap<String, Service>();

    public SovereignBuilder allCoreServices() {
        PresenceService presenceService = new PresenceService();
        serviceMap.put("presence", presenceService);

        ConfService confService = new ConfService();
        serviceMap.put("conf", confService);

        CoordinationService coordService = new CoordinationService();
        serviceMap.put("coord", coordService);

        return this;
    }

    public SovereignBuilder zkConfig(String zkConnectString, int zkSessionTimeout) {
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;
        return this;

    }

    public SovereignBuilder registerService(String serviceName, Service service) {
        serviceMap.put(serviceName, service);
        return this;
    }

    public SovereignBuilder pathCache(int pathCacheSize, int pathCacheConcurrencyLevel) {
        this.pathCacheSize = pathCacheSize;
        this.pathCacheConcurrencyLevel = pathCacheConcurrencyLevel;
        return this;
    }

    public Sovereign build() {
        Sovereign s = new Sovereign(zkConnectString, zkSessionTimeout, pathCacheSize, pathCacheConcurrencyLevel);
        s.registerServices(serviceMap);
        return s;
    }

}
