package io.reign;

import io.reign.util.JacksonUtil;

import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.type.TypeReference;

/**
 * Default path scheme.
 * 
 * @author ypai
 * 
 */
public class DefaultPathScheme implements PathScheme {

    private static final Pattern PATTERN_PATH_TOKENIZER = Pattern.compile("/");

    private String basePath;

    public DefaultPathScheme() {

    }

    public DefaultPathScheme(String basePath) {
        this();
        this.basePath = basePath;

    }

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

        return basePath + "/" + pathType + "/" + relativePath;
    }

    @Override
    public String getAbsolutePath(PathType pathType, String... pathTokens) {
        return getAbsolutePath(pathType, joinTokens(pathTokens));
    }

    @Override
    public String getParentPath(String path) {
        if (!isValidPath(path)) {
            throw new IllegalArgumentException("Invalid path:  path=" + path);
        }
        return path.substring(0, path.lastIndexOf("/"));
    }

    @Override
    public String joinPaths(String... paths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];

            if (!isValidPath(path)) {
                throw new IllegalArgumentException("Invalid path:  path=" + path);
            }

            // strip trailing slash
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }

            // only add slash btw. paths if we have to
            if (i > 0 && !path.startsWith("/")) {
                sb.append('/');
            }

            sb.append(path);
        }

        return sb.toString();

    }

    @Override
    public String[] tokenizePath(String path) {
        path = path.replaceFirst("^/", "");
        return PATTERN_PATH_TOKENIZER.split(path);
    }

    @Override
    public String joinTokens(String... pathTokens) {
        StringBuilder sb = new StringBuilder();
        for (String token : pathTokens) {
            if (!isValidPathToken(token)) {
                throw new IllegalArgumentException("Invalid path token:  pathToken='" + token + "'");

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

    @Override
    public boolean isValidPath(String path) {
        return !path.endsWith("/");
    }

    @Override
    public CanonicalId parseCanonicalId(String canonicalId) {
        try {
            return JacksonUtil.getObjectMapperInstance().readValue(canonicalId,
                    new TypeReference<DefaultCanonicalId>() {
                    });
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse '" + canonicalId + "':  " + e, e);
        }
    }

    @Override
    public String toPathToken(CanonicalId canonicalId) {
        try {
            return JacksonUtil.getObjectMapperInstance().writeValueAsString(canonicalId);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
