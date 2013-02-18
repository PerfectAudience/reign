package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public String getBasePath();

    public String getInternalBasePath();

    public String getInternalAbsolutePath(PathType pathType);

    public String getInternalAbsolutePath(PathType pathType, String relativePath);

    public String getAbsolutePath(PathType pathType);

    public String getAbsolutePath(PathType pathType, String relativePath);

    public String[] tokenizePath(String path);

    public String buildRelativePath(String... pathTokens);

    public boolean isValidPathToken(String pathToken);
}
