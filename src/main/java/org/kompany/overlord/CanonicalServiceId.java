package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public class CanonicalServiceId {

    private final String clusterId;

    private final String serviceId;

    public static CanonicalServiceId fromPathString(PathScheme pathScheme, String relativePath) {
        String[] tokens = pathScheme.tokenizePath(relativePath);
        if (tokens.length != 2) {
            throw new IllegalArgumentException("Invalid node path:  path=" + relativePath);
        }
        return new CanonicalServiceId(tokens[0], tokens[1]);
    }

    public CanonicalServiceId(String clusterId, String serviceId) {
        super();
        if (clusterId == null || serviceId == null) {
            throw new IllegalArgumentException("clusterId and/or serviceId cannot be null!");
        }
        this.clusterId = clusterId;
        this.serviceId = serviceId;

    }

    public String toPathString(PathScheme pathScheme) {
        return pathScheme.buildRelativePath(clusterId, serviceId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj instanceof CanonicalServiceId)) {
            return false;
        }

        CanonicalServiceId csi = (CanonicalServiceId) obj;
        return csi.toString().equals(this.toString());

    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public String toString() {
        return clusterId + "/" + serviceId;
    }

}
