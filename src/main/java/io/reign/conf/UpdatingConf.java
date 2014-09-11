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

import io.reign.PathType;
import io.reign.ReignContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Utility configuration class that is eventually consistent with latest values in ZooKeeper using an internal observer.
 * 
 * @author ypai
 * 
 */
public class UpdatingConf<K, V> implements Map<K, V> {

	private String clusterId;
	private String relativePath;
	private ReignContext context;
	private ConfObserver<Map<K, V>> observer;
	private volatile Map<K, V> conf;

	public UpdatingConf(String clusterId, String serviceId, String confName, ReignContext context) {
		this(clusterId, context.getPathScheme().joinTokens(serviceId, confName), context);
	}

	public UpdatingConf(String clusterId, String relativePath, ReignContext context) {
		this.clusterId = clusterId;
		this.relativePath = relativePath;
		this.context = context;
		this.observer = new ConfObserver<Map<K, V>>() {
			@Override
			public void updated(Map<K, V> updated, Map<K, V> existing) {
				conf = updated;
			}
		};

		ConfService confService = context.getService("conf");
		conf = confService.getConf(clusterId, relativePath, observer);
		if (conf == null) {
			conf = Collections.EMPTY_MAP;
		}
	}

	public void destroy() {
		String path = context.getPathScheme().getAbsolutePath(PathType.CONF, clusterId, relativePath);
		context.getObserverManager().remove(path, observer);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("Not supported!");
	}

	@Override
	public boolean containsKey(Object arg0) {
		return conf.containsKey(arg0);
	}

	@Override
	public boolean containsValue(Object arg0) {
		return conf.containsValue(arg0);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return conf.entrySet();
	}

	@Override
	public V get(Object arg0) {
		return conf.get(arg0);
	}

	@Override
	public boolean isEmpty() {
		return conf.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return conf.keySet();
	}

	@Override
	public V put(K arg0, V arg1) {
		throw new UnsupportedOperationException("Not supported!");
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> arg0) {
		throw new UnsupportedOperationException("Not supported!");
	}

	@Override
	public V remove(Object arg0) {
		throw new UnsupportedOperationException("Not supported!");
	}

	@Override
	public int size() {
		return conf.size();
	}

	@Override
	public Collection<V> values() {
		return conf.values();
	}

}
