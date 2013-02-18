package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    /**
     * 
     * @param basePath
     *            absolute path
     */
    public void setBasePath(String basePath);

    public String getBasePath();

    public String getAbsolutePath(PathType pathType);

    public String getAbsolutePath(PathType pathType, String relativePath);

    public String[] tokenizePath(String path);

    public String buildRelativePath(String... pathTokens);

    public boolean isValidPathToken(String pathToken);
}
