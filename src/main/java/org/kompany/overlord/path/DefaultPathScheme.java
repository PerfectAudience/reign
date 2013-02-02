package org.kompany.overlord.path;


/**
 * Default path scheme.
 * 
 * @author ypai
 * 
 */
public class DefaultPathScheme implements PathScheme {

    private String basePath = "/overlord";

    @Override
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public String getBasePath() {
        return basePath;
    }

    @Override
    public String getAbsolutePath(PathType pathType) {
        return basePath + "/" + pathType;
    }

    @Override
    public String getAbsolutePath(PathType pathType, String relativePath) {
        // TODO Auto-generated method stub
        return basePath + "/" + pathType + "/" + relativePath;
    }

}
