/*
 * Copyright 2013 Yen Pai ypai@reign.io
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.reign.conf;

import io.reign.AbstractService;
import io.reign.DataSerializer;
import io.reign.JsonDataSerializer;
import io.reign.PathType;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.mesg.ResponseStatus;
import io.reign.mesg.SimpleResponseMessage;
import io.reign.util.ZkClientUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote/centralized configuration service.
 * 
 * @author ypai
 * 
 */
public class ConfService extends AbstractService {

    static final DataSerializer DEFAULT_CONF_SERIALIZER = new JsonDataSerializer();

    private static final Logger logger = LoggerFactory.getLogger(ConfService.class);

    private final ZkClientUtil zkUtil = new ZkClientUtil();

    // private final Map<String, DataSerializer> dataSerializerMap = new ConcurrentHashMap<String, DataSerializer>(17,
    // 0.9f, 1);

    public ConfService() {
        // dataSerializerMap.put("json", new JsonDataSerializer());
        // dataSerializerMap.put("js", dataSerializerMap.get("json"));
        // dataSerializerMap.put("properties", dataSerializerMap.get("json"));
        // dataSerializerMap.put("properties", new ConfPropertiesSerializer<ConfProperties>(false));
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {
    }

    // /**
    // *
    // * @param extension
    // * "file" extension of path: would be "properties" in the following path:
    // * /my-cluster/my-config.properties
    // * @param dataSerializer
    // */
    // public void registerSerializer(String extension, DataSerializer dataSerializer) {
    // dataSerializerMap.put(extension, dataSerializer);
    // }

    static DataSerializer getDataSerializer(String path, Map<String, DataSerializer> dataSerializerMap) {
        int lastDotIndex = path.lastIndexOf(".");
        String extension = path.substring(lastDotIndex + 1);
        DataSerializer result = dataSerializerMap.get(extension);
        if (result == null) {
            throw new IllegalArgumentException("Could not derive serializer from given information:  path=" + path);
        }
        return result;
    }

    static boolean isValidConfPath(String path) {
        if (path == null) {
            return false;
        }

        int lastSlashIndex = path.lastIndexOf("/");
        int lastDotIndex = path.lastIndexOf(".");
        if (lastDotIndex == -1 || lastSlashIndex > lastDotIndex || lastDotIndex - lastSlashIndex < 2
                || lastDotIndex < 1 || lastDotIndex == path.length() - 1) {
            return false;
        }
        return true;
    }

    static void throwExceptionIfInvalidConfPath(String path) {
        if (!isValidConfPath(path)) {
            throw new IllegalArgumentException("Invalid configuration path:  path=" + path);
        }
    }

    public <T> void observe(String clusterId, String relativeConfPath, ConfObserver<T> observer) {
        observe(PathType.CONF, clusterId, relativeConfPath, observer);
    }

    <T> void observe(PathType pathType, String clusterId, String relativeConfPath, ConfObserver<T> observer) {
        throwExceptionIfInvalidConfPath(relativeConfPath);

        String absolutePath = getPathScheme().getAbsolutePath(pathType,
                getPathScheme().joinPaths(clusterId, relativeConfPath));

        observer.setClusterId(clusterId);
        // observer.setDataSerializerMap(dataSerializerMap);

        getObserverManager().put(absolutePath, observer);
    }

    /**
     * Picks serializer based on path "file extension".
     * 
     * @param <T>
     * @param clusterId
     * @param relativeConfPath
     * @return
     */
    public <T> T getConf(String clusterId, String relativeConfPath) {
        throwExceptionIfInvalidConfPath(relativeConfPath);

        return getConf(clusterId, relativeConfPath, null);

    }

    public <T> T getConf(String clusterId, String relativeConfPath, ConfObserver<T> observer) {
        throwExceptionIfInvalidConfPath(relativeConfPath);

        // DataSerializer confSerializer = getDataSerializer(relativeConfPath, dataSerializerMap);

        if (observer != null) {
            observe(clusterId, relativeConfPath, observer);
        }

        return (T) getConfAbsolutePath(PathType.CONF, clusterId, relativeConfPath, DEFAULT_CONF_SERIALIZER, null);

    }

    public <T> void putConf(String clusterId, String relativeConfPath, T conf) {
        putConf(clusterId, relativeConfPath, conf, null);
    }

    /**
     * Picks serializer based on path "file extension".
     * 
     * @param <T>
     * @param clusterId
     * @param relativeConfPath
     * @param conf
     */
    public <T> void putConf(String clusterId, String relativeConfPath, T conf, ConfObserver<T> observer) {
        // DataSerializer confSerializer = getDataSerializer(relativeConfPath, dataSerializerMap);
        putConfAbsolutePath(
                getPathScheme().getAbsolutePath(PathType.CONF, getPathScheme().joinPaths(clusterId, relativeConfPath)),
                conf, DEFAULT_CONF_SERIALIZER, getDefaultZkAclList());

        if (observer != null) {
            observe(clusterId, relativeConfPath, observer);
        }
    }

    /**
     * 
     * @param relativePath
     */
    public void removeConf(String clusterId, String relativeConfPath) {
        String path = getPathScheme().getAbsolutePath(PathType.CONF,
                getPathScheme().joinPaths(clusterId, relativeConfPath));

        try {
            getZkClient().delete(path, -1);

        } catch (KeeperException e) {
            if (e.code() != Code.NONODE) {
                logger.error("removeConf():  error trying to delete node:  " + e + ":  path=" + path, e);
            }
        } catch (Exception e) {
            logger.error("removeConf():  error trying to delete node:  " + e + ":  path=" + path, e);
        }
    }

    /**
     * 
     * @param <T>
     * @param pathContext
     * @param pathType
     * @param clusterId
     * @param relativeConfPath
     * @param confSerializer
     * @param observer
     * @param useCache
     * @return
     */
    <T> T getConfAbsolutePath(PathType pathType, String clusterId, String relativeConfPath,
            DataSerializer<T> confSerializer, ConfObserver<T> observer) {

        String absolutePath = getPathScheme().getAbsolutePath(pathType,
                getPathScheme().joinPaths(clusterId, relativeConfPath));
        return getConfAbsolutePath(absolutePath, confSerializer, observer);

    }

    /**
     * 
     * @param <T>
     * @param absolutePath
     * @param confSerializer
     * @param observer
     * @param useCache
     * @return
     */
    <T> T getConfAbsolutePath(String absolutePath, DataSerializer<T> confSerializer, ConfObserver<T> observer) {

        T result = getConfValue(absolutePath, confSerializer);

        /** add observer if given **/
        if (observer != null) {
            getObserverManager().put(absolutePath, observer);

        }

        return result;
    }

    <T> T getConfValue(String absolutePath, DataSerializer<T> confSerializer) {

        throwExceptionIfInvalidConfPath(absolutePath);

        byte[] bytes = null;
        T result = null;

        try {

            // not in cache, so load from ZK
            Stat stat = new Stat();
            bytes = getZkClient().getData(absolutePath, true, stat);

            result = bytes != null ? confSerializer.deserialize(bytes) : null;

        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                // set up watch on that node
                try {
                    getZkClient().exists(absolutePath, true);
                } catch (Exception e1) {
                    logger.error("getConfValue():  error trying to watch node:  " + e1 + ":  path=" + absolutePath, e1);
                }

                logger.debug("getConfValue():  error trying to fetch node info:  {}:  node does not exist:  path={}",
                        e.getMessage(), absolutePath);
            } else {
                logger.error("getConfValue():  error trying to fetch node info:  " + e, e);
            }
        } catch (Exception e) {
            logger.error("getConfValue():  error trying to fetch node info:  " + e, e);
        }

        return result;
    }

    /**
     * 
     * @param <T>
     * @param pathContext
     * @param pathType
     * @param clusterId
     * @param relativeConfPath
     * @param conf
     * @param confSerializer
     * @param aclList
     */
    <T> void putConfAbsolutePath(String absolutePath, T conf, DataSerializer<T> confSerializer, List<ACL> aclList) {

        throwExceptionIfInvalidConfPath(absolutePath);

        try {
            // write to ZK
            byte[] leafData = confSerializer.serialize(conf);
            String pathUpdated = zkUtil.updatePath(getZkClient(), getPathScheme(), absolutePath, leafData, aclList,
                    CreateMode.PERSISTENT, -1);

            logger.debug("putConfAbsolutePath():  saved configuration:  path={}", pathUpdated);
        } catch (Exception e) {
            logger.error("putConfAbsolutePath():  error while saving configuration:  " + e + ":  path=" + absolutePath,
                    e);
        }
    }

    void putConfAbsolutePath(String absolutePath, byte[] bytes, List<ACL> aclList) {

        throwExceptionIfInvalidConfPath(absolutePath);

        try {
            // write to ZK
            String pathUpdated = zkUtil.updatePath(getZkClient(), getPathScheme(), absolutePath, bytes, aclList,
                    CreateMode.PERSISTENT, -1);

            logger.debug("putConfAbsolutePath():  saved configuration:  path={}", pathUpdated);
        } catch (Exception e) {
            logger.error("putConfAbsolutePath():  error while saving configuration:  " + e + ":  path=" + absolutePath,
                    e);
        }
    }

    @Override
    public ResponseMessage handleMessage(RequestMessage requestMessage) {
        ResponseMessage responseMessage = new SimpleResponseMessage();
        try {
            /** preprocess request **/
            String requestBody = (String) requestMessage.getBody();
            int newlineIndex = requestBody.indexOf("\n");
            String resourceLine = requestBody;
            String confBody = null;
            if (newlineIndex != -1) {
                resourceLine = requestBody.substring(0, newlineIndex);
                confBody = requestBody.substring(newlineIndex + 1);
            }

            // get meta
            String meta = null;
            String resource = null;
            int hashLastIndex = resourceLine.lastIndexOf("#");
            if (hashLastIndex != -1) {
                meta = resourceLine.substring(hashLastIndex + 1);
                resource = resourceLine.substring(0, hashLastIndex);
            } else {
                resource = resourceLine;
            }

            // get resource; strip beginning and ending slashes "/"
            if (resource.startsWith("/")) {
                resource = resource.substring(1);
            }
            if (resource.endsWith("/")) {
                resource = resource.substring(0, resource.length() - 1);
            }

            /** get response **/
            String[] tokens = getPathScheme().tokenizePath(resource);

            String clusterId = null;
            String relativePath = null;
            if (tokens.length == 1) {
                // list configurations in cluster
                clusterId = tokens[0];

            } else if (tokens.length > 1) {
                clusterId = tokens[0];

                String[] relativePathTokens = Arrays.<String> copyOfRange(tokens, 1, tokens.length);
                relativePath = getPathScheme().joinTokens(relativePathTokens);

            }

            if (logger.isDebugEnabled()) {
                logger.debug("clusterId={}; relativePath={}; meta={}", new Object[] { clusterId, relativePath, meta });
            }

            /** decide what to do based on meta value **/
            if (meta == null) {
                // get conf or list conf(s) available
                if (clusterId != null && relativePath != null) {
                    if (isValidConfPath(relativePath)) {
                        Object conf = getConf(clusterId, relativePath);
                        if (conf != null) {
                            responseMessage.setBody(conf);
                        }
                    } else {
                        // list items available
                        String absolutePath = getPathScheme().getAbsolutePath(PathType.CONF,
                                getPathScheme().joinPaths(clusterId, relativePath));
                        List<String> childList = getZkClient().getChildren(absolutePath, false);
                        responseMessage.setBody(childList);
                    }

                } else if (clusterId != null && relativePath == null) {
                    // list configs in cluster
                    String absolutePath = getPathScheme().getAbsolutePath(PathType.CONF, clusterId);
                    List<String> childList = getZkClient().getChildren(absolutePath, false);
                    responseMessage.setBody(childList);

                } else {
                    // both clusterId and relativePath are null: just list available clusters
                    String absolutePath = getPathScheme().getAbsolutePath(PathType.CONF);
                    List<String> childList = getZkClient().getChildren(absolutePath, false);
                    responseMessage.setBody(childList);
                }

            } else if ("update".equals(meta) || "put".equals(meta)) {
                // edit conf (add, remove, or update properties)
                Map conf;
                if ("put".equals(meta)) {
                    logger.info("PUT configuration:  clusterId={}; path={}", clusterId, relativePath);

                    conf = new HashMap<String, Object>();

                } else {
                    logger.info("UPDATE configuration:  clusterId={}; path={}", clusterId, relativePath);

                    conf = getConf(clusterId, relativePath);

                    // set existing configuration in response now so even if we get unexpected error, it will show in
                    // response
                    responseMessage.setBody(conf);

                    // create one to build upon if doesn't exist yet
                    if (conf == null) {
                        // if this is a new configuration
                        conf = new HashMap<String, Object>();
                    }
                }

                // TODO: use UTF-8 universally in the future?
                // read in api body as properties for easier processing
                Map<String, String> updateConf = new HashMap<String, String>();
                if (confBody != null) {
                    String[] confBodyLines = confBody.split("\n");
                    for (String confLine : confBodyLines) {
                        int equalsIndex = confLine.indexOf("=");
                        if (equalsIndex != -1) {
                            String key = confLine.substring(0, equalsIndex).trim();
                            String value = confLine.substring(equalsIndex + 1).trim();
                            updateConf.put(key, value);
                        } else {
                            logger.debug("Could not parse line:  ignoring:  '" + confLine + "'");
                        }
                    }
                }
                boolean isUpdate = "update".equals(meta);
                for (Object keyObject : updateConf.keySet()) {
                    String key = (String) keyObject;
                    if (isUpdate && key.startsWith("+")) {
                        String newKey = key.substring(1);

                        // only add if doesn't already exist
                        if (conf.get(newKey) == null) {
                            conf.put(newKey, castValueIfNecessary(updateConf.get(key)));
                        }

                    } else if (isUpdate && key.startsWith("-")) {
                        key = key.substring(1);

                        // remove key
                        conf.remove(key);
                    } else {
                        // add or overwrite existing property
                        conf.put(key, castToMatchExistingTypeIfNecessary(updateConf.get(key), conf.get(key)));
                    }
                }// for

                putConf(clusterId, relativePath, conf);

                // set if it's a "put"
                responseMessage.setBody(conf);

            } else if ("delete".equals(meta)) {
                logger.info("DELETE configuration:  clusterId={}; path={}", clusterId, relativePath);

                // delete conf
                removeConf(clusterId, relativePath);

            } else {
                responseMessage.setComment("Invalid request (do not understand '" + meta + "'):  " + requestBody);
            }

        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                if (logger.isDebugEnabled()) {
                    logger.debug("" + e, e);
                }
                responseMessage.setStatus(ResponseStatus.OK, "No configuration found.");
            } else {
                responseMessage.setStatus(ResponseStatus.ERROR_UNEXPECTED, "" + e);
            }
        } catch (Exception e) {
            logger.error("handleMessage():  " + e, e);
            responseMessage.setStatus(ResponseStatus.ERROR_UNEXPECTED, "" + e);
        }

        responseMessage.setId(requestMessage.getId());

        return responseMessage;
    }

    /**
     * 
     * @param value
     * @param matchValue
     * @return object of the same type as matchValue
     */
    static Object castToMatchExistingTypeIfNecessary(String value, Object matchValue) {
        // try to cast value
        Object castedValue = castValueIfNecessary(value);

        if (matchValue == null || castedValue == null || !value.equals(castedValue) || matchValue instanceof String) {
            // favor explicit cast
            return castedValue;
        } else if (matchValue instanceof Integer) {
            return Integer.parseInt(value);
        } else if (matchValue instanceof Long) {
            return Long.parseLong(value);
        } else if (matchValue instanceof Double) {
            return Double.parseDouble(value);
        } else if (matchValue instanceof Float) {
            return Float.parseFloat(value);
        } else if (matchValue instanceof Short) {
            return Short.parseShort(value);
        } else if (matchValue instanceof Byte) {
            return Byte.parseByte(value);
        }

        logger.warn("matchType():  cannot handle type, so defaulting to String:  {}", matchValue.getClass().getName());
        return value;
    }

    /**
     * 
     * @param value
     * @param matchValue
     * @return object of the same type as matchValue
     */
    static Object castValueIfNecessary(String value) {
        if (value == null) {
            return value;
        } else if (value.startsWith("(int)")) {
            return Integer.parseInt(value.substring(5));
        } else if (value.startsWith("(long)")) {
            return Long.parseLong(value.substring(6));
        } else if (value.startsWith("(double)")) {
            return Double.parseDouble(value.substring(8));
        } else if (value.startsWith("(float)")) {
            return Float.parseFloat(value.substring(7));
        } else if (value.startsWith("(short)")) {
            return Short.parseShort(value.substring(7));
        } else if (value.startsWith("(byte)")) {
            return Byte.parseByte(value.substring(6));
        } else if (value.startsWith("(string)")) {
            return value.substring(8);
        } else {
            return value;
        }

    }

}