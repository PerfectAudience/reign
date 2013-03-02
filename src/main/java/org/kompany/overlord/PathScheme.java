package org.kompany.overlord;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public String getBasePath(PathContext pathContext);

    public String getAbsolutePath(PathContext pathContext, PathType pathType);

    public String getAbsolutePath(PathContext pathContext, PathType pathType, String relativePath);

    public String getAbsolutePath(PathContext pathContext, PathType pathType, String... pathTokens);

    public String join(String pathSegment1, String pathSegment2);

    public String[] tokenizePath(String path);

    public String buildRelativePath(String... pathTokens);

    public boolean isValidPathToken(String pathToken);
}
