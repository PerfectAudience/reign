package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public String getBasePath();

    public String getAbsolutePath(PathType pathType);

    public String getAbsolutePath(PathType pathType, String relativePath);

    public String getAbsolutePath(PathType pathType, String... pathTokens);

    public String join(String pathSegment1, String pathSegment2);

    public String[] tokenizePath(String path);

    public String buildRelativePath(String... pathTokens);

    public boolean isValidPathToken(String pathToken);

    // public String getCanonicalId();
    //
    // public String getCanonicalId(Integer port);

    /**
     * Try to parse input String as canonical ID with some embedded information
     * 
     * @param canonicalId
     * @return Map of values; or null
     * @throws IllegalArgumentException
     *             if there is a parsing error
     */
    public CanonicalId parseCanonicalId(String canonicalId);

    /**
     * 
     * @param canonicalId
     * @return valid path representation of CanonicalId
     */
    public String toPathToken(CanonicalId canonicalId);
}
