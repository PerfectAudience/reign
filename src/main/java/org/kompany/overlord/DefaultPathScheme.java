package org.kompany.overlord;

import java.util.regex.Pattern;

/**
 * Default path scheme.
 * 
 * @author ypai
 * 
 */
public class DefaultPathScheme implements PathScheme {

    private static final Pattern PATTERN_PATH_TOKENIZER = Pattern.compile("/");

    private String basePath;

    private String internalBasePath;

    public DefaultPathScheme() {

    }

    public DefaultPathScheme(String basePath, String internalBasePath) {
        this();
        this.basePath = basePath;
        this.internalBasePath = internalBasePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public void setInternalBasePath(String basePath) {
        this.internalBasePath = internalBasePath;
    }

    @Override
    public String getInternalBasePath() {
        return internalBasePath;
    }

    @Override
    public String getInternalAbsolutePath(PathType pathType) {
        return internalBasePath + "/" + pathType;
    }

    @Override
    public String getInternalAbsolutePath(PathType pathType, String relativePath) {
        return internalBasePath + "/" + pathType + "/" + relativePath;
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
        return basePath + "/" + pathType + "/" + relativePath;
    }

    @Override
    public String[] tokenizePath(String path) {
        return PATTERN_PATH_TOKENIZER.split(path);
    }

    @Override
    public String buildRelativePath(String... pathTokens) {
        StringBuilder sb = new StringBuilder();
        for (String token : pathTokens) {
            if (!isValidPathToken(token)) {
                throw new IllegalArgumentException("'/' character is not allowed in path token:  pathToken='" + token
                        + "'");

            }
            sb.append(token);
            sb.append('/');
        }
        return sb.substring(0, sb.length() - 1);
    }

    @Override
    public boolean isValidPathToken(String pathToken) {
        return pathToken.indexOf('/') == -1;
    }

}
