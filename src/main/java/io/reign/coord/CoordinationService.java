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

package io.reign.coord;

import io.reign.AbstractService;
import io.reign.PathType;
import io.reign.conf.ConfService;
import io.reign.mesg.ParsedRequestMessage;
import io.reign.mesg.RequestMessage;
import io.reign.mesg.ResponseMessage;
import io.reign.mesg.ResponseStatus;
import io.reign.mesg.SimpleResponseMessage;
import io.reign.util.JacksonUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides resources for coordination like locks, semaphores, barriers, etc.
 * 
 * @author ypai
 * 
 */
public class CoordinationService extends AbstractService {
	private static final Logger logger = LoggerFactory.getLogger(CoordinationService.class);

	private ZkReservationManager zkReservationManager;

	/**
	 * what is the maximum amount of time a reservation can be held: -1 for indefinite (used to kill reservations held
	 * by zombie processes as a safety)
	 */
	private volatile long maxReservationHoldTimeMillis = -1;

	private final CoordinationServiceCache coordinationServiceCache = new CoordinationServiceCache();

	private ScheduledThreadPoolExecutor executorService;

	public CoordinationService() {
		super();

	}

	@Override
	public synchronized void init() {
		if (executorService != null) {
			return;
		}

		zkReservationManager = new ZkReservationManager(getZkClient(), getPathScheme(), coordinationServiceCache);

		executorService = new ScheduledThreadPoolExecutor(1);

		Runnable adminRunnable = new AdminRunnable();
		executorService.scheduleAtFixedRate(adminRunnable, 60000, 60000, TimeUnit.MILLISECONDS);

	}

	@Override
	public void destroy() {
		zkReservationManager.shutdown();
		executorService.shutdown();

	}

	public List<String> getLocks(String clusterId) {
		String path = getPathScheme().getAbsolutePath(PathType.COORD, clusterId,
		        ReservationType.LOCK_EXCLUSIVE.category());
		List<String> children = Collections.EMPTY_LIST;
		try {
			Stat stat = new Stat();
			children = getZkClient().getChildren(path, true, stat);

		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				logger.warn("getLocks():  " + e + ":  node does not exist:  path={}", path);
			} else {
				logger.warn("getLocks():  " + e, e);
			}
			return Collections.EMPTY_LIST;
		} catch (InterruptedException e) {
			logger.warn("lookupClusters():  " + e, e);
			return Collections.EMPTY_LIST;
		}

		return children != null ? children : Collections.EMPTY_LIST;
	}

	public List<String> getBarriers(String clusterId) {
		String path = getPathScheme().getAbsolutePath(PathType.COORD, clusterId, ReservationType.BARRIER.category());
		List<String> children = Collections.EMPTY_LIST;
		try {
			Stat stat = new Stat();
			children = getZkClient().getChildren(path, true, stat);

		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				logger.warn("getBarriers():  " + e + ":  node does not exist:  path={}", path);
			} else {
				logger.warn("getBarriers():  " + e, e);
			}
			return Collections.EMPTY_LIST;
		} catch (InterruptedException e) {
			logger.warn("lookupClusters():  " + e, e);
			return Collections.EMPTY_LIST;
		}

		return children != null ? children : Collections.EMPTY_LIST;
	}

	public List<String> getSemaphores(String clusterId) {
		String path = getPathScheme().getAbsolutePath(PathType.COORD, clusterId, ReservationType.SEMAPHORE.category());
		List<String> children = Collections.EMPTY_LIST;
		try {
			Stat stat = new Stat();
			children = getZkClient().getChildren(path, true, stat);

		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				logger.warn("getSemaphores():  " + e + ":  node does not exist:  path={}", path);
			} else {
				logger.warn("getSemaphores():  " + e, e);
			}
			return Collections.EMPTY_LIST;
		} catch (InterruptedException e) {
			logger.warn("lookupClusters():  " + e, e);
			return Collections.EMPTY_LIST;
		}

		return children != null ? children : Collections.EMPTY_LIST;
	}

	public void observe(String clusterId, String lockName, LockObserver observer) {
		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.LOCK_EXCLUSIVE, lockName);
		observer.setCoordinationServiceCache(coordinationServiceCache);
		observer.setPathScheme(getPathScheme());
		getObserverManager().put(entityPath, observer);
	}

	public void observe(String clusterId, String semaphoreName, SemaphoreObserver observer) {
		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.SEMAPHORE, semaphoreName);
		observer.setCoordinationServiceCache(coordinationServiceCache);
		observer.setPathScheme(getPathScheme());
		getObserverManager().put(entityPath, observer);
	}

	public DistributedBarrier getBarrier(String clusterId, String barrierName, int parties) {
		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.BARRIER, barrierName);
		return new ZkDistributedBarrier(entityPath, getContext().getNodeId().toString(), parties, getContext());
	}

	public DistributedReentrantLock getReentrantLock(String clusterId, String lockName) {
		return getReentrantLock(clusterId, lockName, getDefaultZkAclList());
	}

	DistributedReentrantLock getReentrantLock(String clusterId, String lockName, List<ACL> aclList) {
		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.LOCK_EXCLUSIVE, lockName);
		DistributedLock lock = new ZkReentrantLock(zkReservationManager, getContext().getNodeId().toString(),
		        entityPath, ReservationType.LOCK_EXCLUSIVE, aclList);
		this.coordinationServiceCache.putLock(entityPath, ReservationType.LOCK_EXCLUSIVE, lock);

		return (DistributedReentrantLock) lock;
	}

	public DistributedLock getLock(String clusterId, String lockName) {
		return getLock(clusterId, lockName, getDefaultZkAclList());
	}

	DistributedLock getLock(String clusterId, String lockName, List<ACL> aclList) {
		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.LOCK_EXCLUSIVE, lockName);
		DistributedLock lock = new ZkLock(zkReservationManager, getContext().getNodeId().toString(), entityPath,
		        ReservationType.LOCK_EXCLUSIVE, aclList);
		this.coordinationServiceCache.putLock(entityPath, ReservationType.LOCK_EXCLUSIVE, lock);

		return lock;
	}

	public DistributedReadWriteLock getReadWriteLock(String clusterId, String lockName) {
		return getReadWriteLock(clusterId, lockName, getDefaultZkAclList());
	}

	DistributedReadWriteLock getReadWriteLock(String clusterId, String lockName, List<ACL> aclList) {

		// write lock
		String writeEntityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.LOCK_EXCLUSIVE, lockName);
		DistributedLock writeLock = new ZkReentrantLock(zkReservationManager, getContext().getNodeId().toString(),
		        writeEntityPath, ReservationType.LOCK_EXCLUSIVE, aclList);
		this.coordinationServiceCache.putLock(writeEntityPath, ReservationType.LOCK_EXCLUSIVE, writeLock);

		// read lock
		String readEntityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.LOCK_SHARED, lockName);
		DistributedLock readLock = new ZkReentrantLock(zkReservationManager, getContext().getNodeId().toString(),
		        readEntityPath, ReservationType.LOCK_SHARED, aclList);
		this.coordinationServiceCache.putLock(readEntityPath, ReservationType.LOCK_SHARED, readLock);

		return new ZkReadWriteLock(readLock, writeLock);
	}

	public DistributedSemaphore getFixedSemaphore(String clusterId, String semaphoreName, int permitPoolSize) {
		return getSemaphore(clusterId, semaphoreName, new ConstantPermitPoolSize(permitPoolSize), getDefaultZkAclList());
	}

	DistributedSemaphore getFixedSemaphore(String clusterId, String semaphoreName, int permitPoolSize, List<ACL> aclList) {
		return getSemaphore(clusterId, semaphoreName, new ConstantPermitPoolSize(permitPoolSize), aclList);
	}

	public DistributedSemaphore getConfiguredSemaphore(String clusterId, String semaphoreName) {
		return getConfiguredSemaphore(clusterId, semaphoreName, -1, false, getDefaultZkAclList());
	}

	DistributedSemaphore getConfiguredSemaphore(String ownerId, String clusterId, String semaphoreName,
	        List<ACL> aclList) {
		return getConfiguredSemaphore(clusterId, semaphoreName, -1, false, aclList);
	}

	public DistributedSemaphore getConfiguredSemaphore(String clusterId, String semaphoreName, int permitPoolSize,
	        boolean createConfigurationIfNecessary) {
		return getConfiguredSemaphore(clusterId, semaphoreName, permitPoolSize, createConfigurationIfNecessary,
		        getDefaultZkAclList());
	}

	DistributedSemaphore getConfiguredSemaphore(String clusterId, String semaphoreName, int permitPoolSize,
	        boolean createConfigurationIfNecessary, List<ACL> aclList) {

		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.SEMAPHORE, semaphoreName);

		/**
		 * create permit pool size function and add observer to adjust as necessary
		 **/
		ConfService confService = getContext().getService("conf");
		PermitPoolSize pps = new ConfiguredPermitPoolSize(confService, clusterId, semaphoreName, permitPoolSize,
		        createConfigurationIfNecessary);
		pps.initialize();

		/** create and cache **/
		DistributedSemaphore semaphore = getSemaphore(clusterId, semaphoreName, pps, aclList);
		coordinationServiceCache.putSemaphore(entityPath, semaphore);

		return semaphore;
	}

	DistributedSemaphore getSemaphore(String clusterId, String semaphoreName, PermitPoolSize permitPoolSize,
	        List<ACL> aclList) {

		String entityPath = CoordServicePathUtil.getAbsolutePathEntity(getPathScheme(), PathType.COORD, clusterId,
		        ReservationType.SEMAPHORE, semaphoreName);

		// check to see if permit pool size is already in cache
		PermitPoolSize pps = coordinationServiceCache.getPermitPoolSize(entityPath);
		if (pps == null) {
			pps = coordinationServiceCache.putOrReturnCachedPermitPoolSize(entityPath, permitPoolSize);
		} else {
			if (pps.getClass() != permitPoolSize.getClass()) {
				throw new IllegalStateException("PermitPoolSize already exists but is not of type "
				        + permitPoolSize.getClass().getName() + ":  clusterId=" + clusterId + "; semaphoreName="
				        + semaphoreName);
			}
		}

		// create and put in cache
		DistributedSemaphore semaphore = new ZkSemaphore(zkReservationManager, getContext().getNodeId().toString(),
		        entityPath, aclList, pps);
		coordinationServiceCache.putSemaphore(entityPath, semaphore);

		return semaphore;
	}

	public long getMaxReservationHoldTimeMillis() {
		return maxReservationHoldTimeMillis;
	}

	public void setMaxReservationHoldTimeMillis(long maxReservationHoldTimeMillis) {
		this.maxReservationHoldTimeMillis = maxReservationHoldTimeMillis;
	}

	public ResponseMessage handleMessage(RequestMessage requestMessage) {
		ResponseMessage responseMessage = new SimpleResponseMessage();
		responseMessage.setId(requestMessage.getId());

		try {
			if (logger.isTraceEnabled()) {
				logger.trace("Received message:  nodeId={}; request='{}:{}'", requestMessage.getSenderId(),
				        requestMessage.getTargetService(), requestMessage.getBody());
			}

			/** preprocess request **/
			ParsedRequestMessage parsedRequestMessage = new ParsedRequestMessage(requestMessage);
			String resource = parsedRequestMessage.getResource();

			// strip beginning and ending slashes "/"
			if (resource.startsWith("/")) {
				resource = resource.substring(1);
			}
			if (resource.endsWith("/")) {
				resource = resource.substring(0, resource.length() - 1);
			}

			/** get response **/
			logger.debug("resource={}; meta={}", resource, parsedRequestMessage.getMeta());

			// if base path, just return available clusters
			if (parsedRequestMessage.getMeta() == null) {

				if (resource.length() == 0) {
					// list available clusters
					String path = getContext().getPathScheme().getAbsolutePath(PathType.COORD);
					List<String> clusterList = getContext().getZkClient().getChildren(path, false);
					responseMessage.setBody(clusterList);

				} else {
					String[] tokens = getPathScheme().tokenizePath(resource);
					// logger.debug("tokens.length={}", tokens.length);

					if (tokens.length == 1) {
						// list coordination entity types
						String path = getContext().getPathScheme().getAbsolutePath(PathType.COORD, tokens[0]);
						List<String> entityTypes = getContext().getZkClient().getChildren(path, false);
						responseMessage.setBody(entityTypes);

					} else if (tokens.length == 2) {
						// list nodes for a given coordination entity
						String path = getContext().getPathScheme()
						        .getAbsolutePath(PathType.COORD, tokens[0], tokens[1]);
						List<String> entityList = getContext().getZkClient().getChildren(path, false);
						responseMessage.setBody(entityList);

					} else if (tokens.length == 3) {
						// list reservation nodes for a given coordination entity
						String path = getContext().getPathScheme().getAbsolutePath(PathType.COORD, tokens[0],
						        tokens[1], tokens[2]);
						List<String> reservationList = getContext().getZkClient().getChildren(path, false);
						responseMessage.setBody(reservationList);

					} else if (tokens.length == 4) {
						// get owner data
						String path = getContext().getPathScheme().getAbsolutePath(PathType.COORD, tokens[0],
						        tokens[1], tokens[2], tokens[3]);
						byte[] bytes = getContext().getZkClient().getData(path, false, new Stat());
						Map<String, String> ownerData = JacksonUtil.getObjectMapper().readValue(bytes, Map.class);
						responseMessage.setBody(ownerData);
					}
				}
			}// if parsedRequestMessage.getMeta() == null

			return responseMessage;

		} catch (Exception e) {
			logger.error("" + e, e);
			responseMessage = new SimpleResponseMessage(ResponseStatus.ERROR_UNEXPECTED, requestMessage.getId());
			responseMessage.setComment("" + e);
			return responseMessage;
		}
	}

	public class AdminRunnable implements Runnable {
		@Override
		public void run() {
			/** get exclusive leader lock to perform maintenance duties **/
			DistributedLock adminLock = getLock("reign", "coord-reaper", getDefaultZkAclList());
			adminLock.lock();
			logger.info("Performing administrative maintenance...");
			try {
				/** semaphore maintenance **/
				// list semaphores

				// acquire lock on a semaphore

				// revoke any permits that have exceeded the limit

				/** lock maintenance **/
				// list locks

				// get exclusive lock on a given lock to perform long held lock
				// checking

				// traverse lock tree and remove any long held locks that exceed
				// threshold

				/** barrier maintenance **/
			} finally {
				adminLock.unlock();
				adminLock.destroy();
			}
		}// run
	}// AdminRunnable
}
