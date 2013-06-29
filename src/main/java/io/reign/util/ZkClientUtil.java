package io.reign.util;

import io.reign.PathScheme;
import io.reign.ZkClient;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience functions to make operating with Zookeeper easier.
 * 
 * @author ypai
 * 
 */
public class ZkClientUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZkClientUtil.class);

    /**
     * Creates path, including parent nodes if necessary.
     * 
     * @param path
     * @param data
     * @param acl
     * @param createMode
     * @return
     * @throws KeeperException
     */
    public String updatePath(final ZkClient zkClient, final PathScheme pathScheme, final String path,
            final byte[] leafData, final List<ACL> aclList, final CreateMode createMode, int leafDataVersion)
            throws KeeperException {

        /***** if there is leaf data, try updating first to save on calls to ZK *****/
        if (!createMode.isSequential()) {
            try {
                zkClient.setData(path, leafData, leafDataVersion);
                return path;
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NONODE) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("updatePath():  path does not exist for data update:  " + e + ":  path=" + path);
                    }
                } else {
                    logger.error("updatePath():  " + e, e);
                    throw e;
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted in updatePath():  " + e, e);

            }// try/catch
        }// if

        /***** try to build path without building parents to save on calls to ZK *****/
        try {

            String pathCreated = zkClient.create(path, leafData, aclList, createMode);

            if (logger.isDebugEnabled()) {
                logger.debug("Created path directly:  pathCreated={}", pathCreated);
            }

            return pathCreated;
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Path already exists:  " + e + ": path=" + path);
                }

                return path;
            } else if (e.code() == KeeperException.Code.NONODE) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Parent path does not exist:  " + e + ":  path=" + path);
                }
            } else {
                logger.error("Error while building path:  " + e + "; path=" + path, e);
                throw e;
            }// if

        } catch (InterruptedException e) {
            logger.error(e + "", e);
            logger.warn("Interrupted in updatePath():  " + e, e);
        }// try/catch

        /***** build path by building parent nodes first *****/
        String[] tokens = pathScheme.tokenizePath(path);

        // StringBuilder pathInProgress = new StringBuilder();
        String pathCreated = "";
        String pathToCreate = null;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if ("".equals(token)) {
                // we should never get here, and if we are getting here, log as
                // error
                logger.warn("updatePath():  token is empty string!:  path='{}'; i={}; pathCreated='{}'", new Object[] {
                        path, i, pathCreated });

            } else {
                try {
                    pathToCreate = pathCreated + "/" + token;

                    if (logger.isDebugEnabled()) {
                        logger.debug("Creating node:  pathToCreate={}; token={}", pathToCreate, token);
                    }

                    // default to persistent mode until leaf node, then we use
                    // the preferred create mode of caller
                    CreateMode currentCreateMode = CreateMode.PERSISTENT;
                    byte[] nodeData = null;
                    if (i == tokens.length - 1) {
                        currentCreateMode = createMode;
                        nodeData = leafData;
                    }

                    pathCreated = zkClient.create(pathToCreate, nodeData, aclList, currentCreateMode);

                } catch (KeeperException e) {
                    if (e.code() == KeeperException.Code.NODEEXISTS) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Path already exists:  path={}", pathToCreate);
                        }

                        pathCreated = pathToCreate;
                    } else {
                        logger.error("Error while building path:  " + e + ":  path=" + pathToCreate, e);
                        throw e;
                    }// if

                } catch (InterruptedException e) {
                    logger.error(e + "", e);
                    logger.warn("Interrupted in updatePath():  " + e, e);
                }// try/catch
            }// if
        }// for

        if (logger.isDebugEnabled()) {
            logger.debug("Created path by building parent nodes:  pathCreated={}", pathCreated);
        }
        return pathCreated;

    }

}
