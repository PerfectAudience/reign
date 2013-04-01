package org.kompany.sovereign.data;

import org.kompany.sovereign.AbstractActiveService;

/**
 * 
 * @author ypai
 * 
 */
public class DataService extends AbstractActiveService {

    public NodeDatum getDatum(String clusterId, String serviceId, String nodeId) {
        return null;
    }

    public ServiceDatum getDatum(String clusterId, String serviceId) {
        return null;
    }

    public <T> void emit(String clusterId, String serviceId, String nodeId, SimpleDataItem<T> dataItem) {

    }

    @Override
    public void perform() {
        // TODO Auto-generated method stub

    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

}
