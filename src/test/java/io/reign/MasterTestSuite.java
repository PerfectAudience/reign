package io.reign;

import io.reign.data.DataServiceTestSuite;
import io.reign.presence.PresenceServiceTestSuite;

import java.io.File;
import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Suite.class)
@SuiteClasses({ PresenceServiceTestSuite.class, DataServiceTestSuite.class })
public class MasterTestSuite {

    private static final Logger logger = LoggerFactory.getLogger(MasterTestSuite.class);

    private static TestingServer zkTestServer;

    private static Reign reign;

    public static final int ZK_TEST_SERVER_PORT = 21810;

    public static Reign getReign() {
        return reign;
    }

    @BeforeClass
    public static void setUpClass() {

        /** bootstrap a real ZooKeeper instance **/
        logger.debug("Starting Test ZooKeeper server...");
        try {
            String dataDirectory = System.getProperty("java.io.tmpdir");
            File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
            zkTestServer = new TestingServer(ZK_TEST_SERVER_PORT, dir);
        } catch (Exception e) {
            logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
        }

        /** init and start reign using builder **/
        reign = Reign.maker().messagingPort(33133).zkClient("localhost:" + MasterTestSuite.ZK_TEST_SERVER_PORT, 30000)
                .pathCache(1024, 8).core().get();
        reign.start();
    }

    @AfterClass
    public static void tearDownClass() {

        /** stop reign **/
        reign.stop();

        /** shutdown ZooKeeper instance **/
        try {
            logger.debug("Stopping Test ZooKeeper server...");
            zkTestServer.stop();
        } catch (IOException e) {
            logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
        }
    }
}