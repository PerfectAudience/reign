package io.reign.coord;

import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

public class ZkLockTest {

	private CoordinationService coordinationService;
	private ThreadPoolExecutor executorService;

	@Before
	public void setUp() throws Exception {
		coordinationService = MasterTestSuite.getReign().getService("coord");
		executorService = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

	}

	@Test
	public void testTryLock() throws Exception {
		final AtomicInteger acquiredCount = new AtomicInteger(0);

		Runnable t1 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("reign", "testTryLock");
				try {
					if (lock.tryLock()) {
						acquiredCount.incrementAndGet();
						Thread.sleep(10000);
					}
				} catch (Exception e1) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};

		Runnable t2 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("reign", "testTryLock");
				try {
					if (lock.tryLock()) {
						acquiredCount.incrementAndGet();
					}
				} catch (Exception e1) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};

		Runnable t3 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("reign", "testTryLock");
				try {
					if (lock.tryLock()) {
						acquiredCount.incrementAndGet();
					}
				} catch (Exception e1) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};
		Runnable t4 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("reign", "testTryLock");
				try {
					if (lock.tryLock()) {
						acquiredCount.incrementAndGet();
					}
				} catch (Exception e1) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};

		executorService.submit(t1);
		executorService.submit(t2);
		executorService.submit(t3);
		executorService.submit(t4);
		while (executorService.getCompletedTaskCount() < 4) {
			Thread.sleep(1000);
		}
		assertTrue(acquiredCount.get() == 1);
	}

	@Test
	public void testDistributedLock() throws Exception {

		// use StringBuffer because it is synchronized and thread-safe
		final AtomicInteger count = new AtomicInteger(0);

		// threads to simulate multiple processes
		final Runnable t1 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("clusterA", "test-lock-1");
				lock.lock();
				try {
					int current = count.get();
					Thread.sleep((int) (10000 * Math.random()) + 15000);
					count.set(current + 1);
				} catch (Exception e) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};
		Runnable t2 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("clusterA", "test-lock-1");
				lock.lock();
				try {
					int current = count.get();
					Thread.sleep((int) (10000 * Math.random()));
					count.set(current + 1);
				} catch (Exception e) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};
		Runnable t3 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("clusterA", "test-lock-1");
				lock.lock();
				try {
					int current = count.get();
					Thread.sleep((int) (10000 * Math.random()));
					count.set(current + 1);
				} catch (Exception e) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};
		final AtomicBoolean t4DidNotAcquire = new AtomicBoolean(false);
		Runnable t4 = new Runnable() {
			@Override
			public void run() {
				DistributedLock lock = coordinationService.getLock("clusterA", "test-lock-1");
				if (!lock.tryLock()) {
					t4DidNotAcquire.set(true);
					return;
				}
				try {
					int current = count.get();
					Thread.sleep((int) (10000 * Math.random()));
					count.set(current + 1);
				} catch (Exception e) {
				} finally {
					lock.unlock();
					lock.destroy();
				}
			}
		};

		executorService.submit(t1);
		executorService.submit(t2);
		executorService.submit(t3);
		executorService.submit(t4);
		while (executorService.getCompletedTaskCount() < 4) {
			Thread.sleep(1000);
		}

		assertTrue(t4DidNotAcquire.get());
		assertTrue("Unexpected value:  " + count.get(), count.get() == 3);

	}
}