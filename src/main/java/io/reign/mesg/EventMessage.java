package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public interface EventMessage {

    public String getType();

    public String getClusterId();

    public String getServiceId();

    public String getNodeId();

    public Object getBody();

    public EventMessage setType(String type);

    public EventMessage setClusterId(String clusterId);

    public EventMessage setServiceId(String serviceId);

    public EventMessage setNodeId(String nodeId);

    public EventMessage setBody(Object body);

}
