package io.reign;

import io.reign.conf.ConfServiceTestSuite;
import io.reign.coord.CoordServiceTestSuite;
import io.reign.data.DataServiceTestSuite;
import io.reign.metrics.MetricsServiceTest;
import io.reign.presence.PresenceServiceTestSuite;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Suite.class)
@SuiteClasses({ PresenceServiceTestSuite.class, DataServiceTestSuite.class, CoordServiceTestSuite.class,
        ConfServiceTestSuite.class, MetricsServiceTest.class })
public class MasterTestSuite {

    private static final Logger logger = LoggerFactory.getLogger(MasterTestSuite.class);

    private static TestingServer zkTestServer;

    private static Reign reign;

    public static final int ZK_TEST_SERVER_PORT = 21810;

    public static ExecutorService executorService = Executors.newFixedThreadPool(10);

    public static ExecutorService getExecutorService() {
        return executorService;
    }

    public static synchronized Reign getReign() {
        setUpClass();
        return reign;
    }

    @BeforeClass
    public static void setUpClass() {

        if (reign != null) {
            return;
        }

        /** bootstrap a real ZooKeeper instance **/
        logger.debug("Starting Test ZooKeeper server...");
        try {
            String dataDirectory = System.getProperty("java.io.tmpdir");
            if (!dataDirectory.endsWith("/")) {
                dataDirectory += File.separator;
            }
            dataDirectory += UUID.randomUUID().toString();
            logger.debug("ZK dataDirectory={}", dataDirectory);
            File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
            zkTestServer = new TestingServer(ZK_TEST_SERVER_PORT, dir);
        } catch (Exception e) {
            logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
        }

        /** init and start reign using builder **/
        reign = Reign.maker().messagingPort(33133).zkClient("localhost:" + MasterTestSuite.ZK_TEST_SERVER_PORT, 30000)
                .pathCache(1024, 8).get();
        reign.start();

    }

    @AfterClass
    public static void tearDownClass() {
        try {
            // wait a bit for any async tasks to finish
            Thread.sleep(5000);

            // shut down utility executor
            executorService.shutdown();

            // stop reign
            reign.stop();

            logger.debug("Stopping Test ZooKeeper server...");
            zkTestServer.stop();

        } catch (Exception e) {
            logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
        }
    }
}