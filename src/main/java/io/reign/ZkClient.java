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

package io.reign;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Defines the framework's interface to Zookeeper.
 * 
 * @author ypai
 * 
 */
public interface ZkClient {

    public void register(Watcher watcher);

    public void close();

    public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException;

    public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException;

    public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException;

    public List<String> getChildren(final String path, final Watcher watcher) throws KeeperException,
            InterruptedException;

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException,
            InterruptedException;

    public Stat setData(final String path, final byte[] data, final int version) throws KeeperException,
            InterruptedException;

    public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException,
            InterruptedException;

    public String create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode)
            throws KeeperException, InterruptedException;

    public void delete(final String path, final int version) throws InterruptedException, KeeperException;

    public void sync(final String path, final VoidCallback cb, final Object ctx);
}
