package io.reign;

/**
 * The ZooKeeper representation of NodeId
 * 
 * @author ypai
 * 
 */
public class ZkNodeId {

    private final String pathToken;

    private final byte[] data;

    public ZkNodeId(String pathToken, byte[] data) {
        this.pathToken = pathToken;
        this.data = data;
    }

    public String getPathToken() {
        return pathToken;
    }

    public byte[] getData() {
        return data;
    }

}
