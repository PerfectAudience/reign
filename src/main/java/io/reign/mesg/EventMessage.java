package io.reign.mesg;

/**
 * 
 * @author ypai
 * 
 */
public interface EventMessage {

    public String getEvent();

    public String getClusterId();

    public String getServiceId();

    public String getNodeId();

    public Object getBody();

    public EventMessage setEvent(String event);

    public EventMessage setClusterId(String clusterId);

    public EventMessage setServiceId(String serviceId);

    public EventMessage setNodeId(String nodeId);

    public EventMessage setBody(Object body);

}
