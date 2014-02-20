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

import io.reign.util.JacksonUtil;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * @author ypai
 * 
 */
public class DefaultCanonicalId implements CanonicalId {

    /**
     * process ID
     */
    @JsonProperty("pid")
    private String processId;

    /**
     * IP address of node
     */
    @JsonProperty("ip")
    private String ipAddress;

    /**
     * host name of node
     */
    @JsonProperty("h")
    private String host;

    // /**
    // * the port for an available service (short-cut to specifying it as an node attribute -- fewer ZK operations if
    // this
    // * is all that is needed to interact with a service once discovered)
    // */
    // @JsonProperty("p")
    // private Integer port;

    /** the messaging port for the framework */
    @JsonProperty("mp")
    private Integer messagingPort;

    public DefaultCanonicalId() {

    }

    public DefaultCanonicalId(String processId, String ipAddress, String host, Integer messagingPort) {
        this.processId = processId;
        this.ipAddress = ipAddress;
        this.host = host;
        // this.port = port;
        this.messagingPort = messagingPort;
    }

    // public static DefaultCanonicalId id() {
    // return new DefaultCanonicalId();
    // }

    // @Override
    // public DefaultCanonicalId port(Integer port) {
    // setPort(port);
    // return this;
    // }

    @Override
    public String getProcessId() {
        return processId;
    }

    // @Override
    // public void setProcessId(String processId) {
    // this.processId = processId;
    // }

    @Override
    public String getIpAddress() {
        return ipAddress;
    }

    // @Override
    // public void setIpAddress(String ipAddress) {
    // this.ipAddress = ipAddress;
    // }

    @Override
    public String getHost() {
        return host;
    }

    // @Override
    // public void setHost(String host) {
    // this.host = host;
    // }

    // @Override
    // public Integer getPort() {
    // return port;
    // }

    // @Override
    // public void setPort(Integer port) {
    // this.port = port;
    // }

    @Override
    public Integer getMessagingPort() {
        return messagingPort;
    }

    // @Override
    // public void setMessagingPort(Integer messagingPort) {
    // this.messagingPort = messagingPort;
    // }

    @Override
    public String toString() {
        try {
            return JacksonUtil.getObjectMapperInstance().writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
