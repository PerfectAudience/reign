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

    public static final String CANONICAL_ID_PID = "pid";
    public static final String CANONICAL_ID_IP = "ip";
    public static final String CANONICAL_ID_HOST = "host";
    public static final String CANONICAL_ID_PORT = "port";
    public static final String CANONICAL_ID_MESSAGING_PORT = "mport";

    private static final Pattern PATTERN_PATH_TOKENIZER = Pattern.compile("/");

    private String basePath;

    // private Integer messagingPort;

    // private final String canonicalId;

    public DefaultPathScheme() {
        // this.canonicalId = defaultCanonicalId();
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
        return getAbsolutePath(pathType, buildRelativePath(pathTokens));
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

    // @Override
    // public String getCanonicalId() {
    // StringBuilder sb = new StringBuilder(this.canonicalId);
    // if (this.messagingPort != null) {
    // sb.insert(sb.length() - 1, ",\"").insert(sb.length() - 1, CANONICAL_ID_MESSAGING_PORT).insert(
    // sb.length() - 1, "\":\"").insert(sb.length() - 1, this.messagingPort).insert(sb.length() - 1, "\"");
    // }
    // return sb.toString();
    // }
    //
    // @Override
    // public String getCanonicalId(Integer port) {
    // if (port == null) {
    // throw new IllegalArgumentException("Invalid argument:  port cannot be null!");
    // }
    //
    // StringBuilder sb = new StringBuilder(this.canonicalId);
    // if (this.messagingPort != null) {
    // sb.insert(sb.length() - 1, ",\"").insert(sb.length() - 1, CANONICAL_ID_MESSAGING_PORT).insert(
    // sb.length() - 1, "\":\"").insert(sb.length() - 1, this.messagingPort).insert(sb.length() - 1, "\"");
    // }
    // if (port != null) {
    // sb.insert(sb.length() - 1, ",\"").insert(sb.length() - 1, CANONICAL_ID_PORT).insert(sb.length() - 1,
    // "\":\"").insert(sb.length() - 1, port).insert(sb.length() - 1, "\"");
    // }
    // return sb.toString();
    // }

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

    // String defaultCanonicalId() {
    // // get pid
    // String pid = IdUtil.getProcessId();
    //
    // // try to get hostname and ip address
    // String hostname = IdUtil.getHostname();
    // String ipAddress = IdUtil.getIpAddress();
    //
    // // fill in unknown values
    // if (pid == null) {
    // pid = "";
    // }
    // if (hostname == null) {
    // hostname = "";
    // }
    // if (ipAddress == null) {
    // ipAddress = "";
    // }
    //
    // return "{\"" + CANONICAL_ID_HOST + "\":\"" + hostname + "\",\"" + CANONICAL_ID_IP + "\":\"" + ipAddress
    // + "\",\"" + CANONICAL_ID_PID + "\":\"" + pid + "\"}";
    // }

}
