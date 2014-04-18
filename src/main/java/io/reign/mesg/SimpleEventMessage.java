package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public class SimpleEventMessage implements EventMessage {

    private String type = null;
    private String clusterId = null;
    private String serviceId = null;
    private String nodeId = null;
    private Object body = null;

    @Override
    public String getType() {
        return type;
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
    public EventMessage setType(String type) {
        this.type = type;
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
