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
public class Utf8StringSerializerTest extends AbstractDataSerializerTest {

    private Utf8StringSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new Utf8StringSerializer();
    }

    @Override
    @Test
    public void testBasic() throws Exception {
        String data = "Superduper man with some funky chars:  �";

        byte[] bytes = serializer.serialize(data);

        String reconstituted = serializer.deserialize(bytes);

        assertTrue(data + " != " + reconstituted, data.equals(reconstituted));
    }
}
