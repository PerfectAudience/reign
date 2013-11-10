package io.reign;

import io.reign.data.DataServiceTestSuite;
import io.reign.presence.PresenceServiceTestSuite;

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

    @BeforeClass
    public static void setUpClass() {

        // bootstrap a real ZooKeeper instance
        logger.debug("Starting Test ZooKeeper server...");
        try {
            zkTestServer = new TestingServer(21818);
        } catch (Exception e) {
            logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
        }

    }

    @AfterClass
    public static void tearDownClass() {

        // shutdown ZooKeeper instance
        try {
            logger.debug("Stopping Test ZooKeeper server...");
            zkTestServer.stop();
        } catch (IOException e) {
            logger.error("Trouble starting test ZooKeeper instance:  " + e, e);
        }
    }
}