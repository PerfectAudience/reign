package org.kompany.sovereign;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.type.TypeReference;
import org.kompany.sovereign.util.IdUtil;
import org.kompany.sovereign.util.JacksonUtil;

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

    private final String canonicalId;

    public DefaultPathScheme() {
        this.canonicalId = defaultCanonicalId();
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
        path = path.replaceFirst("^/", "");
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

    @Override
    public String getCanonicalId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCanonicalId(int port) {
        StringBuilder sb = new StringBuilder(this.canonicalId);
        sb.insert(sb.length() - 1, ",\"").insert(sb.length() - 1, CANONICAL_ID_PORT).insert(sb.length() - 1, "\":\"")
                .insert(sb.length() - 1, port).insert(sb.length() - 1, "\"");
        return sb.toString();
    }

    @Override
    public Map<String, String> parseCanonicalId(String canonicalId) {
        try {
            return JacksonUtil.getObjectMapperInstance().readValue(canonicalId,
                    new TypeReference<Map<String, String>>() {
                    });
        } catch (Exception e) {
            throw new UnexpectedSovereignException(e);
        }
    }

    static String defaultCanonicalId() {
        // get pid
        String pid = IdUtil.getProcessId();

        // try to get hostname and ip address
        String hostname = IdUtil.getHostname();
        String ipAddress = IdUtil.getIpAddress();

        // fill in unknown values
        if (pid == null) {
            pid = "";
        }
        if (hostname == null) {
            hostname = "";
        }
        if (ipAddress == null) {
            ipAddress = "";
        }

        return "{\"" + CANONICAL_ID_HOST + "\":\"" + hostname + "\",\"" + CANONICAL_ID_IP + "\":\"" + ipAddress
                + "\",\"" + CANONICAL_ID_PID + "\":\"" + pid + "\"}";
    }

}
