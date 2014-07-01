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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.reign.MasterTestSuite;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ypai
 * 
 */
public class ConfServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfServiceTest.class);

    private ConfService confService;

    @Before
    public void setUp() throws Exception {
        confService = MasterTestSuite.getReign().getService("conf");
    }

    @Test
    public void testPutConfAndGetConf() {
        Properties props1 = new Properties();
        props1.put("foo", "bar");
        props1.put("wizard", "oz");

        confService.putConf("clusterA", "test1.properties", props1);

        Map<String, Object> props2 = confService.getConf("clusterA", "test1.properties");
        assertTrue(props1.get("foo").equals(props2.get("foo")));
        assertTrue(props1.get("wizard").equals(props2.get("wizard")));
        assertTrue("bar".equals(props1.get("foo")));
        assertTrue("oz".equals(props1.get("wizard")));
    }

    @Test
    public void testObserver() throws Exception {
        Map<String, Object> props1 = new HashMap<String, Object>();
        props1.put("foo", "bar");
        props1.put("wizard", "oz");
        confService.putConf("clusterA", "test2.properties", props1);

        final AtomicBoolean testPassed = new AtomicBoolean(false);
        confService.observe("clusterA", "test2.properties", new ConfObserver<Map<String, Object>>() {
            @Override
            public void updated(Map<String, Object> updated, Map<String, Object> existing) {
                if ("kid".equals(updated.get("sundance")) && existing.get("sundance") == null && updated.size() == 3
                        && existing.size() == 2) {
                    testPassed.set(true);
                }
                synchronized (testPassed) {
                    testPassed.notifyAll();
                }
            }
        });

        props1.put("sundance", "kid");
        confService.putConf("clusterA", "test2.properties", props1);

        synchronized (testPassed) {
            testPassed.wait(10000);
        }
        assertTrue(testPassed.get());
    }

    @Test
    public void testIsValidConfPath() {
        assertTrue(ConfService.isValidConfPath("this.properties"));
        assertTrue(ConfService.isValidConfPath("/conf/what/super/a.json"));
        assertTrue(ConfService.isValidConfPath("/conf/what/super/a.0.1.x.json"));
        assertTrue(ConfService.isValidConfPath("/conf/what/this.properties"));
        assertTrue(ConfService.isValidConfPath("/conf/what/super/this.properties"));

        assertFalse(ConfService.isValidConfPath("."));
        assertFalse(ConfService.isValidConfPath("/."));
        assertFalse(ConfService.isValidConfPath("a."));
        assertFalse(ConfService.isValidConfPath(".properties"));
        assertFalse(ConfService.isValidConfPath("/.properties"));
        assertFalse(ConfService.isValidConfPath("/conf/what/super/this.properties/"));
    }

    @Test
    public void testCastValueIfNecessary() {
        Object value;

        value = ConfService.castValueIfNecessary("(int)999");
        assertTrue(value.equals(999) && value instanceof Integer);

        value = ConfService.castValueIfNecessary("(long)9999999999");
        assertTrue(value.equals(9999999999L) && value instanceof Long);

        value = ConfService.castValueIfNecessary("(double)9.99");
        assertTrue(value.equals(9.99) && value instanceof Double);

        value = ConfService.castValueIfNecessary("(float)99.9");
        assertTrue(value.equals(99.9F) && value instanceof Float);

        value = ConfService.castValueIfNecessary("(short)9");
        assertTrue(value.equals((short) 9) && value instanceof Short);

        value = ConfService.castValueIfNecessary("(byte)1");
        assertTrue(value.equals((byte) 1) && value instanceof Byte);
    }
}
