package io.reign.mesg;

import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * 
 * @author ypai
 * 
 */
@JsonPropertyOrder({ "event", "clusterId", "serviceId", "nodeId", "body" })
public class SimpleEventMessage implements EventMessage {

    private String event = null;
    private String clusterId = null;
    private String serviceId = null;
    private String nodeId = null;
    private Object body = null;

    @Override
    public String getEvent() {
        return event;
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public Object getBody() {
        return body;
    }

    @Override
    public EventMessage setEvent(String event) {
        this.event = event;
        return this;
    }

    @Override
    public EventMessage setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    @Override
    public EventMessage setServiceId(String serviceId) {
        this.serviceId = serviceId;
        return this;
    }

    @Override
    public EventMessage setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    @Override
    public EventMessage setBody(Object body) {
        this.body = body;
        return this;
    }
}
