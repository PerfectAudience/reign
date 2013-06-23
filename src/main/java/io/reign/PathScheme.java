package io.reign;

import java.util.Map;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public static final String CANONICAL_ID_PID = "pid";
    public static final String CANONICAL_ID_IP = "ip";
    public static final String CANONICAL_ID_HOST = "host";
    public static final String CANONICAL_ID_PORT = "port";

    public String getBasePath();

    public String getAbsolutePath(PathType pathType);

    public String getAbsolutePath(PathType pathType, String relativePath);

    public String getAbsolutePath(PathType pathType, String... pathTokens);

    public String join(String pathSegment1, String pathSegment2);

    public String[] tokenizePath(String path);

    public String buildRelativePath(String... pathTokens);

    public boolean isValidPathToken(String pathToken);

    public String getCanonicalId();

    public String getCanonicalId(int port);

    /**
     * Try to parse input String as canonical ID with some embedded information
     * 
     * @param canonicalId
     * @return Map of values; or null
     */
    public Map<String, String> parseCanonicalId(String canonicalId);
}
