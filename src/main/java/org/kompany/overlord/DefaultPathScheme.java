package org.kompany.overlord;

import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

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

    public void setInternalBasePath(String internalBasePath) {
        this.internalBasePath = internalBasePath;
    }

    @Override
    public String getBasePath(PathContext pathContext) {
        if (pathContext == PathContext.INTERNAL) {
            return internalBasePath;
        } else if (pathContext == PathContext.USER) {
            return basePath;
        }
        throw new IllegalArgumentException("Invalid path context:  pathContext=" + pathContext);
    }

    @Override
    public String getAbsolutePath(PathContext pathContext, PathType pathType) {
        String basePath = getBasePath(pathContext);
        return basePath + "/" + pathType;
    }

    @Override
    public String getAbsolutePath(PathContext pathContext, PathType pathType, String relativePath) {
        String basePath = getBasePath(pathContext);
        return basePath + "/" + pathType + "/" + relativePath;
    }

    @Override
    public String getAbsolutePath(PathContext pathContext, PathType pathType, String... pathTokens) {
        return getAbsolutePath(pathContext, pathType, buildRelativePath(pathTokens));
    }

    @Override
    public String join(String pathSegment1, String pathSegment2) {
        return pathSegment1 + '/' + pathSegment2;
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
        return !StringUtils.isBlank(pathToken) && pathToken.indexOf('/') == -1;
    }

}
