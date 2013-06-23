package io.reign.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for generating node identifiers.
 * 
 * @author ypai
 * 
 */
public class IdUtil {

    private static final Logger logger = LoggerFactory.getLogger(IdUtil.class);

    public static String getIpAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.warn("Could not get IP address:  " + e, e);
            return null;
        }
    }

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName().toString();
        } catch (UnknownHostException e) {
            logger.warn("Could not get hostname:  " + e, e);
            return null;
        }
    }

    public static String getProcessId() {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        String[] ids = jvmName.split("@");
        return ids[0];
    }

}
