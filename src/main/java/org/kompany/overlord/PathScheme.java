package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public String getBasePath(PathContext context);

    public String getAbsolutePath(PathContext context, PathType pathType);

    public String getAbsolutePath(PathContext context, PathType pathType, String relativePath);

    public String[] tokenizePath(String path);

    public String buildRelativePath(String... pathTokens);

    public boolean isValidPathToken(String pathToken);
}
