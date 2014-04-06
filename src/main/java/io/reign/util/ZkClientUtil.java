/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign.util;

import io.reign.PathScheme;
import io.reign.ZkClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
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

    public String updatePath(final ZkClient zkClient, final PathScheme pathScheme, final String path,
            final byte[] leafData, final List<ACL> aclList, final CreateMode createMode, int leafDataVersion)
            throws KeeperException {
        return updatePath(zkClient, pathScheme, path, leafData, aclList, createMode, leafDataVersion, null);
    }

    public void syncPath(final ZkClient zkClient, String dataPath, final Object monitorObject) {
        logger.trace("Syncing ZK client:  dataPath={}", dataPath);
        zkClient.sync(dataPath, new VoidCallback() {
            @Override
            public void processResult(int arg0, String arg1, Object arg2) {
                if (monitorObject != null) {
                    synchronized (monitorObject) {
                        monitorObject.notifyAll();
                    }
                }
            }
        }, null);

        // wait for sync to complete
        if (monitorObject != null) {
            synchronized (monitorObject) {
                try {
                    logger.trace("Waiting for ZK client sync complete:  dataPath={}", dataPath);
                    monitorObject.wait();
                    logger.trace("ZK client sync completed:  dataPath={}", dataPath);
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting for ZK sync():  " + e, e);
                }
            }
        }
    }

    /**
     * 
     * @param zkClient
     * @param pathScheme
     * @param path
     * @param leafData
     * @param aclList
     * @param createMode
     * @param leafDataVersion
     * @param statRef
     *            to return Stat on update of data nodes
     * @return
     * @throws KeeperException
     */
    public String updatePath(final ZkClient zkClient, final PathScheme pathScheme, final String path,
            final byte[] leafData, final List<ACL> aclList, final CreateMode createMode, int leafDataVersion,
            AtomicReference<Stat> statRef) throws KeeperException {

        /***** if there is leaf data, try updating first to save on ZK ops *****/
        if (!createMode.isSequential()) {
            try {
                if (statRef != null) {
                    statRef.set(zkClient.setData(path, leafData, leafDataVersion));
                } else {
                    zkClient.setData(path, leafData, leafDataVersion);
                }
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
