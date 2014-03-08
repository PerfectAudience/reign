package io.reign.examples;

import io.reign.Reign;
import io.reign.presence.PresenceObserver;
import io.reign.presence.PresenceService;
import io.reign.presence.ServiceInfo;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReignServer {
    private static final Logger logger = LoggerFactory.getLogger(ReignServer.class);

    public static void main(String[] args) throws Exception {
        /** init and start reign using builder **/
        Reign reign = Reign.maker().zkClient("localhost:2181", 30000).pathCache(1024, 8).get();
        reign.start();

        PresenceService presenceService = reign.getService("presence");
        presenceService.observe("rtb", "bidder", new PresenceObserver<ServiceInfo>() {
            @Override
            public void updated(ServiceInfo info) {
                if (info != null) {
                    logger.info("***** Observer:  serviceInfo={}",
                            ReflectionToStringBuilder.toString(info, ToStringStyle.DEFAULT_STYLE));
                } else {
                    logger.info("***** Observer:  serviceInfo deleted");
                }
            }
        });
        presenceService.announce("rtb", "bidder", true);

        /** let server run **/
        Object obj = new Object();
        synchronized (obj) {
            obj.wait();
        }

    }
}
