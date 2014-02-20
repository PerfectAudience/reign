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

package io.reign;

import io.reign.util.IdUtil;
import io.reign.util.JacksonUtil;

import org.codehaus.jackson.type.TypeReference;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultCanonicalIdProvider implements CanonicalIdProvider {

    private String processId;
    private String host;
    private String ipAddress;

    private final Integer messagingPort;

    public DefaultCanonicalIdProvider() {
        this(null);
    }

    public DefaultCanonicalIdProvider(Integer messagingPort) {
        this.messagingPort = messagingPort;

        // get pid
        processId = IdUtil.getProcessId();

        // try to get hostname and ip address
        host = IdUtil.getHostname();
        ipAddress = IdUtil.getIpAddress();

        // fill in unknown values
        if (processId == null) {
            processId = "";
        }
        if (host == null) {
            host = "";
        }
        if (ipAddress == null) {
            ipAddress = "";
        }
    }

    @Override
    public CanonicalId get() {
        return new DefaultCanonicalId(processId, ipAddress, host, messagingPort);
    }

    // @Override
    // public CanonicalId get(Integer port) {
    // CanonicalId id = new DefaultCanonicalId(processId, ipAddress, host, port, messagingPort);
    // return id;
    // }

    @Override
    public CanonicalId from(String pathToken, byte[] data) {
        try {
            return JacksonUtil.getObjectMapperInstance().readValue(pathToken, new TypeReference<DefaultCanonicalId>() {
            });
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse '" + pathToken + "':  " + e, e);
        }
    }

}
