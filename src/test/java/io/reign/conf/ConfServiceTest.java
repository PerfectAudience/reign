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

package io.reign.conf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author ypai
 * 
 */
public class ConfServiceTest {

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
