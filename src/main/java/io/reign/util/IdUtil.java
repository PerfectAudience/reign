/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

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
