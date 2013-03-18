package org.kompany.sovereign;

/**
 * 
 * @author ypai
 * 
 */
public class CanonicalNodeId {

    private final String clusterId;

    private final String serviceId;

    private final String nodeId;

    public static CanonicalNodeId fromPathString(PathScheme pathScheme, String relativePath) {
        String[] tokens = pathScheme.tokenizePath(relativePath);
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid node path:  path=" + relativePath);
        }
        return new CanonicalNodeId(tokens[0], tokens[1], tokens[2]);
    }

    public CanonicalNodeId(String clusterId, String serviceId, String nodeId) {
        super();
        if (clusterId == null || serviceId == null || nodeId == null) {
            throw new IllegalArgumentException("clusterId, serviceId, and/or nodeId cannot be null!");
        }
        this.clusterId = clusterId;
        this.serviceId = serviceId;
        this.nodeId = nodeId;
    }

    public String toPathString(PathScheme pathScheme) {
        return pathScheme.buildRelativePath(clusterId, serviceId, nodeId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj instanceof CanonicalNodeId)) {
            return false;
        }

        CanonicalNodeId cni = (CanonicalNodeId) obj;
        return cni.toString().equals(this.toString());

    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public String toString() {
        return clusterId + "/" + serviceId + "/" + nodeId;
    }
}
