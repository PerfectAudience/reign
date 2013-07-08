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

package io.reign.data;

import static org.junit.Assert.assertTrue;
import io.reign.AbstractDataSerializerTest;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author ypai
 * 
 */
public class FloatSerializerTest extends AbstractDataSerializerTest {

    private FloatSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new FloatSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        float data1 = Float.MAX_VALUE;
        float data2 = Float.MIN_VALUE;

        byte[] bytes1;
        byte[] bytes2;
        Float reconstituted;

        // serialize and deserialize multiple times to make sure buffers in serializer are correctly manipulated

        bytes1 = serializer.serialize(data1);
        bytes2 = serializer.serialize(data2);

        reconstituted = serializer.deserialize(bytes1);
        assertTrue(data1 + " != " + reconstituted, data1 == reconstituted);

        reconstituted = serializer.deserialize(bytes1);
        assertTrue(data1 + " != " + reconstituted, data1 == reconstituted);

        bytes2 = serializer.serialize(data2);
        reconstituted = serializer.deserialize(bytes2);
        assertTrue(data2 + " != " + reconstituted, data2 == reconstituted);
    }
}
