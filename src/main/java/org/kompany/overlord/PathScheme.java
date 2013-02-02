package org.kompany.overlord;



/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public void setBasePath(String basePath);

    public String getBasePath();

    public String getAbsolutePath(PathType pathType);

    public String getAbsolutePath(PathType pathType, String relativePath);

}
